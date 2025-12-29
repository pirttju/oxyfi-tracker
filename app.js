// Load environment variables from .env file
require("dotenv").config();

const fs = require("fs");
const WebSocket = require("ws");
const pgp_lib = require("pg-promise");
const monitor = require("pg-monitor");
const Redis = require("ioredis");

const initOptions = {};
monitor.attach(initOptions);
const pgp = pgp_lib(initOptions);

// --- Configuration ---
const OXYFI_API = process.env.OXYFI_API;
const OXYFI_API_KEY = process.env.OXYFI_API_KEY;
const apiUrl = `${OXYFI_API}?v=1&key=${OXYFI_API_KEY}`;
const STATIONARY_SPEED_THRESHOLD = 2;
const PROCESSING_DELAY = 2000;
const BASE_RECONNECT_INTERVAL = 2000;
const MAX_RECONNECT_INTERVAL = 60000;
const MAX_RETRY_DURATION = 30 * 60 * 1000;

// --- State Variables ---
let retryCount = 0;
let firstDisconnectTime = null;
const formationCache = new Map();
const geometryCache = new Map();
const trainDataBuffer = new Map();
const trainProcessingTimers = new Map();
let heartbeatInterval = null;
let locomotiveTypeRules = {};

// --- pg-promise Configuration ---
const cn = {
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || "5432", 10),
  database: process.env.DB_DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
};
const db = pgp(cn);

// --- Tile38 Client Setup ---
const tile38 = {
  client: new Redis({
    host: process.env.TILE38_HOST || "127.0.0.1",
    port: parseInt(process.env.TILE38_PORT || "9851", 10),
  }),
};

// --- NMEA Parsing and Helper Functions (unchanged) ---
function parseOxyfiMessage(raw) {
  const message = raw.toString();
  const parts = message.split(",");
  if (parts[0].endsWith("RMC") && parts.length >= 18) {
    const time = parts[1];
    const date = parts[9];
    let timestamp = null;
    if (time && date) {
      const hh = time.slice(0, 2);
      const mm = time.slice(2, 4);
      const ss = time.slice(4, 6);
      const DD = date.slice(0, 2);
      const MM = date.slice(2, 4);
      const YY = `20${date.slice(4, 6)}`;
      timestamp = `${YY}-${MM}-${DD}T${hh}:${mm}:${ss}Z`;
    }
    const vehicleNumber = parts[14] ? parts[14].split(".")[0] : null;
    const trainInfo = parts[16];
    const gpsData = {
      timestamp: timestamp,
      latitude: nmeaToDecimal(parts[3], parts[4]),
      longitude: nmeaToDecimal(parts[5], parts[6]),
      speed: parseFloat(parts[7]) * 1.852,
      bearing: parseFloat(parts[8]),
      vehicleNumber: vehicleNumber,
    };
    if (!vehicleNumber || !trainInfo) {
      return { vehicleNumber, formations: [], gpsData };
    }
    const formations = trainInfo
      .split(";")
      .map((info) => {
        const [train, dateStr] = info.split("@");
        if (!train || !dateStr) return null;
        const trainParts = train.split(".");
        if (trainParts[1] === "public") {
          return { trainNumber: trainParts[0], departureDate: dateStr };
        }
        return null;
      })
      .filter(Boolean);
    return { vehicleNumber, formations, gpsData };
  }
  return null;
}
function nmeaToDecimal(coord, direction) {
  if (!coord) return 0;
  const degrees = parseInt(coord.substring(0, coord.indexOf(".") - 2), 10);
  const minutes = parseFloat(coord.substring(coord.indexOf(".") - 2));
  let decimal = degrees + minutes / 60;
  if (direction === "S" || direction === "W") {
    decimal = -decimal;
  }
  return decimal;
}
function getFormationsCanonicalString(formations) {
  if (!formations || formations.length === 0) {
    return "";
  }
  return formations
    .map((f) => `${f.departureDate}:${f.trainNumber}`)
    .sort()
    .join(";");
}
function getGeometryCanonicalString(gpsData) {
  if (!gpsData) return "";
  const lat = gpsData.latitude.toFixed(5);
  const lon = gpsData.longitude.toFixed(5);
  const sp = Math.round(gpsData.speed);
  const be = Math.round(gpsData.bearing);
  return `${lat}:${lon}:${sp}:${be}`;
}

/**
 * Determines the locomotive type based on the loaded rules.
 * @param {string} vehicleNumber The vehicle number to check.
 * @returns {string|null} The locomotive type or null if no match is found.
 */
function getLocomotiveType(vehicleNumber) {
  const num = parseInt(vehicleNumber, 10);
  if (isNaN(num)) {
    return null;
  }

  for (const type in locomotiveTypeRules) {
    const rules = locomotiveTypeRules[type];
    // Check ranges first
    for (const range of rules.ranges) {
      if (num >= range.start && num <= range.end) {
        return type;
      }
    }
    // Then check single numbers
    if (rules.singles.has(num)) {
      return type;
    }
  }
  return null;
}

// --- Database Update Functions ---
async function upsertBasicFormation(vehicleNumber, formation, timestamp) {
  if (!formation) return;
  try {
    const locomotiveType = getLocomotiveType(vehicleNumber);
    const insertData = {
      departure_date: formation.departureDate,
      train_number: formation.trainNumber,
      vehicle_number: vehicleNumber,
      locomotive_type: locomotiveType,
      last_modified: timestamp,
    };
    const cs = new pgp.helpers.ColumnSet(Object.keys(insertData), {
      table: new pgp.helpers.TableName({ table: "oxyfi", schema: "trafiklab" }),
    });
    const onConflict =
      " ON CONFLICT (departure_date, train_number, location) DO UPDATE SET " +
      "vehicle_number = EXCLUDED.vehicle_number, last_modified = EXCLUDED.last_modified, locomotive_type = EXCLUDED.locomotive_type";
    const query = pgp.helpers.insert(insertData, cs) + onConflict;
    await db.none(query);
  } catch (error) {
    console.error(
      `[DB-ERROR] Failed in upsertBasicFormation for vehicle ${vehicleNumber}:`,
      error
    );
  }
}
async function updateFormationWithLocation(
  vehicleNumber,
  formation,
  location,
  timestamp
) {
  if (!formation) return;
  try {
    const locomotiveType = getLocomotiveType(vehicleNumber);
    const insertData = {
      departure_date: formation.departureDate,
      train_number: formation.trainNumber,
      vehicle_number: vehicleNumber,
      location: location,
      locomotive_type: locomotiveType,
      last_modified: timestamp,
    };
    const cs = new pgp.helpers.ColumnSet(Object.keys(insertData), {
      table: new pgp.helpers.TableName({ table: "oxyfi", schema: "trafiklab" }),
    });
    const onConflict =
      " ON CONFLICT (departure_date, train_number, location) DO UPDATE SET " +
      "vehicle_number = EXCLUDED.vehicle_number, last_modified = EXCLUDED.last_modified, locomotive_type = EXCLUDED.locomotive_type";
    const query = pgp.helpers.insert(insertData, cs) + onConflict;
    await db.none(query);
  } catch (error) {
    console.error(
      `[DB-ERROR] Failed in updateFormationWithLocation for vehicle ${vehicleNumber}:`,
      error
    );
  }
}

// --- GPS Ordering Logic ---
async function processBufferedTrainData(trainIdentifier) {
  const vehicleDataMap = trainDataBuffer.get(trainIdentifier);
  trainDataBuffer.delete(trainIdentifier);
  trainProcessingTimers.delete(trainIdentifier);
  if (!vehicleDataMap || vehicleDataMap.size < 2) {
    return;
  }
  const vehicleList = Array.from(vehicleDataMap.values());
  const isStationary = vehicleList.every(
    (v) => v.gps.speed < STATIONARY_SPEED_THRESHOLD
  );
  if (!isStationary) {
    return;
  }

  // VERBOSE: Commented out for production
  // console.log(`[GPS Order] Processing stationary train ${trainIdentifier} with ${vehicleList.length} vehicles.`);
  const totalBearing = vehicleList.reduce((sum, v) => sum + v.gps.bearing, 0);
  const avgBearingRad = (totalBearing / vehicleList.length) * (Math.PI / 180);
  const directionVector = {
    x: Math.sin(avgBearingRad),
    y: Math.cos(avgBearingRad),
  };
  const projectedVehicles = vehicleList.map((v) => {
    const projection =
      v.gps.longitude * directionVector.x + v.gps.latitude * directionVector.y;
    return { ...v, projection };
  });
  projectedVehicles.sort((a, b) => a.projection - b.projection);

  console.log(
    `[GPS Order] Determined order for ${trainIdentifier}: ${projectedVehicles
      .map((v) => v.gps.vehicleNumber)
      .join(", ")}`
  );
  for (let i = 0; i < projectedVehicles.length; i++) {
    const vehicle = projectedVehicles[i];
    const location = i + 1;
    for (const formation of vehicle.formations) {
      await updateFormationWithLocation(
        vehicle.gps.vehicleNumber,
        formation,
        location,
        vehicle.gps.timestamp
      );
    }
  }
}

// --- Resilient WebSocket Connection ---
function connect() {
  console.log(`Attempting to connect (Attempt #${retryCount + 1})...`);
  const ws = new WebSocket(apiUrl);
  ws.on("open", () => {
    console.log("Successfully connected. Resetting retry timer.");
    retryCount = 0;
    firstDisconnectTime = null;
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    heartbeatInterval = setInterval(() => {
      ws.ping();
    }, 30000);
  });
  ws.on("pong", () => {
    // VERBOSE: Commented out for production
    // console.log("[Heartbeat] Received pong from server.");
  });
  ws.on("message", async (data) => {
    const parsedData = parseOxyfiMessage(data);
    if (
      parsedData &&
      parsedData.vehicleNumber &&
      parsedData.formations.length > 0
    ) {
      const { vehicleNumber, formations, gpsData } = parsedData;

      // --- Caching Logic for Basic Database Writes ---
      const newFormationString = getFormationsCanonicalString(formations);
      const cachedFormationString = formationCache.get(vehicleNumber);
      if (newFormationString !== cachedFormationString) {
        // Commented out for production
        //console.log(
        //  `[Cache-DB] Formation change for ${vehicleNumber}. Updating DB.`
        //);
        // Update cache before doing anything to avoid a race condition in case of rapid successive messages
        formationCache.set(vehicleNumber, newFormationString);
        for (const formation of formations) {
          await upsertBasicFormation(
            vehicleNumber,
            formation,
            gpsData.timestamp
          );
        }
      }

      // --- Caching Logic for Tile38 Writes ---
      const newGeometryString = getGeometryCanonicalString(gpsData);
      const cachedGeometryString = geometryCache.get(vehicleNumber);
      if (newGeometryString !== cachedGeometryString) {
        try {
          const firstFormation = formations[0];
          const properties = {
            fe: "se",
            ts: Math.round(new Date(gpsData.timestamp).getTime() / 1000),
            be: Math.round(gpsData.bearing),
            sp: Math.round(gpsData.speed),
            ve: `${vehicleNumber}:${firstFormation.trainNumber}-${firstFormation.departureDate}`,
            ro: firstFormation.trainNumber,
            sd: firstFormation.departureDate,
            rt: 2,
          };
          const geometry = [gpsData.latitude || 0, gpsData.longitude || 0];

          /* FIXME: Update TILE38
          tile38.client.send_command(
            "SET",
            "vehicles",
            properties.ve,
            "FIELD",
            "fe",
            properties.fe,
            "FIELD",
            "ts",
            properties.ts,
            "FIELD",
            "be",
            properties.be,
            "FIELD",
            "sp",
            properties.sp,
            "FIELD",
            "ro",
            properties.ro,
            "FIELD",
            "sd",
            properties.sd,
            "FIELD",
            "rt",
            properties.rt,
            "EX",
            300,
            "POINT",
            geometry[0],
            geometry[1]
          );
          */

          geometryCache.set(vehicleNumber, newGeometryString);
        } catch (error) {
          console.error(`[Tile38-ERROR] Failed to send update:`, error);
        }
      }

      // --- Buffer data for the experimental location calculation ---
      const trainIdentifier = `${formations[0].trainNumber}@${formations[0].departureDate}`;
      if (!trainDataBuffer.has(trainIdentifier)) {
        trainDataBuffer.set(trainIdentifier, new Map());
      }
      trainDataBuffer
        .get(trainIdentifier)
        .set(vehicleNumber, { gps: gpsData, formations: formations });
      if (trainProcessingTimers.has(trainIdentifier)) {
        clearTimeout(trainProcessingTimers.get(trainIdentifier));
      }
      const timerId = setTimeout(
        () => processBufferedTrainData(trainIdentifier),
        PROCESSING_DELAY
      );
      trainProcessingTimers.set(trainIdentifier, timerId);
    }
  });
  ws.on("error", (error) => console.error("WebSocket Error:", error.message));
  ws.on("close", (code, reason) => {
    console.log(`WebSocket disconnected. Code: ${code}, Reason: ${reason}`);
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    ws.removeAllListeners();
    if (firstDisconnectTime === null) {
      firstDisconnectTime = Date.now();
    }
    if (Date.now() - firstDisconnectTime > MAX_RETRY_DURATION) {
      console.error(
        `[FATAL] Failed to reconnect within the ${
          MAX_RETRY_DURATION / 60000
        }-minute limit. Shutting down.`
      );
      process.exit(1);
    }
    const nextRetryDelay =
      Math.min(
        MAX_RECONNECT_INTERVAL,
        BASE_RECONNECT_INTERVAL * Math.pow(2, retryCount)
      ) +
      Math.random() * 1000;
    console.log(
      `Attempting to reconnect in ${Math.round(
        nextRetryDelay / 1000
      )} seconds...`
    );
    setTimeout(connect, nextRetryDelay);
    retryCount++;
  });
}

// --- Main application entry point ---
async function main() {
  console.log("--- Starting Application ---");
  if (!OXYFI_API || !OXYFI_API_KEY) {
    console.error(
      "[FATAL] OXYFI_API or OXYFI_API_KEY is not defined. Please check your .env file."
    );
    process.exit(1);
  }
  console.log("✅ Environment variables loaded.");

  // --- Load and PRE-COMPILE locomotive type rules at startup ---
  try {
    const fileContent = fs.readFileSync("./locomotive_types.json", "utf8");
    const rawRules = JSON.parse(fileContent);

    // Process the raw rules into a more efficient structure
    for (const type in rawRules) {
      locomotiveTypeRules[type] = {
        ranges: [],
        singles: new Set(), // Using a Set for fast lookups
      };

      rawRules[type].forEach((rule) => {
        if (rule.includes("-")) {
          const [start, end] = rule.split("-").map((n) => parseInt(n, 10));
          if (!isNaN(start) && !isNaN(end)) {
            locomotiveTypeRules[type].ranges.push({ start, end });
          }
        } else {
          const num = parseInt(rule, 10);
          if (!isNaN(num)) {
            locomotiveTypeRules[type].singles.add(num);
          }
        }
      });
    }
    console.log("✅ Locomotive type rules loaded and compiled successfully.");
  } catch (error) {
    console.error("[FATAL] Could not load or parse locomotive_types.json.");
    console.error("Error details:", error.message);
    process.exit(1);
  }

  console.log(
    `Connecting to Tile38 at ${process.env.TILE38_HOST || "127.0.0.1"}:${
      process.env.TILE38_PORT || "9851"
    }`
  );
  try {
    const connection = await db.connect();
    console.log("✅ Database connection successful.");
    connection.done();
  } catch (error) {
    console.error(
      "[FATAL] Could not connect to the database. Please check your DB credentials in .env file."
    );
    console.error("Error details:", error.message);
    process.exit(1);
  }
  console.log("--- Initialization complete. Connecting to WebSocket... ---");
  connect();
}
main();
