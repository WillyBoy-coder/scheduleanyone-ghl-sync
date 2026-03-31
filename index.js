require("dotenv").config();
const axios = require("axios");
const cron = require("node-cron");
const fs = require("fs");
const { DateTime } = require("luxon");

const mappingFile = "./mapping.json";
let isSyncRunning = false;

function loadMapping() {
  try {
    if (!fs.existsSync(mappingFile)) return {};
    const raw = fs.readFileSync(mappingFile, "utf8").trim();
    if (!raw) return {};
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function saveMapping(data) {
  fs.writeFileSync(mappingFile, JSON.stringify(data, null, 2));
}

function splitName(fullName = "") {
  const parts = String(fullName).trim().split(/\s+/);
  const firstName = parts.shift() || "Unknown";
  const lastName = parts.join(" ");
  return { firstName, lastName };
}

function formatMMDDYYYY(date) {
  const mm = String(date.getMonth() + 1);
  const dd = String(date.getDate());
  const yyyy = date.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function toISOWithOffset(dateStr) {
  if (!dateStr || typeof dateStr !== "string") return null;

  const trimmed = dateStr.trim();
  const parts = trimmed.split(" ");
  if (parts.length !== 2) return null;

  const [datePart, timePart] = parts;
  const datePieces = datePart.split("/");
  const timePieces = timePart.split(":");

  if (datePieces.length !== 3 || timePieces.length !== 2) return null;

  const [year, month, day] = datePieces;
  const [hour, minute] = timePieces;

  try {
    const dt = DateTime.fromObject(
      {
        year: Number(year),
        month: Number(month),
        day: Number(day),
        hour: Number(hour),
        minute: Number(minute)
      },
      { zone: "America/New_York" }
    );

    return dt.toISO();
  } catch {
    return null;
  }
}

function normalizePhone(phone) {
  if (!phone) return "";
  return String(phone).replace(/[^\d+]/g, "").trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getDateWindow() {
  const today = DateTime.now().setZone("America/New_York");
  const lookahead = Number(process.env.LOOKAHEAD_DAYS || 30);

  const end = today.plus({ days: lookahead });

  return {
    start: today.toFormat("M/d/yyyy"),
    end: end.toFormat("M/d/yyyy")
  };
}

async function getScheduleAnyoneData(startDate, endDate) {
  const response = await axios.get(
    "https://www.membershipsalons.com/report/saledetailapi",
    {
      params: {
        apiToken: process.env.SA_TOKEN,
        start: startDate,
        end: endDate
      }
    }
  );

  return Array.isArray(response.data) ? response.data : [];
}

async function findExistingContactByPhone(phone) {
  if (!phone) return null;

  try {
    const response = await axios.get(
      `${process.env.GHL_BASE_URL}/contacts/search/duplicate`,
      {
        headers: {
          Authorization: `Bearer ${process.env.GHL_TOKEN}`,
          Version: "2021-07-28"
        },
        params: {
          locationId: process.env.GHL_LOCATION_ID,
          number: phone
        }
      }
    );

    return response.data?.contact || response.data || null;
  } catch {
    return null;
  }
}

async function createContact(record) {
  const { firstName, lastName } = splitName(record.Customer || "");
  const phone = normalizePhone(record.MobilePhone || "");

  const body = {
    locationId: process.env.GHL_LOCATION_ID,
    firstName,
    lastName,
    phone,
    source: "Schedule Anyone"
  };

  const response = await axios.post(
    `${process.env.GHL_BASE_URL}/contacts/`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-07-28",
        "Content-Type": "application/json"
      }
    }
  );

  return response.data?.contact || response.data;
}

async function getOrCreateContact(record) {
  const phone = normalizePhone(record.MobilePhone || "");

  if (!phone) return null;

  const existing = await findExistingContactByPhone(phone);
  if (existing?.id) return existing;

  try {
    return await createContact(record);
  } catch (error) {
    const duplicateId = error.response?.data?.meta?.contactId;
    if (duplicateId) return { id: duplicateId };
    throw error;
  }
}

async function createAppointment(record, contactId, startTime, endTime) {
  const body = {
    calendarId: process.env.GHL_CALENDAR_ID,
    locationId: process.env.GHL_LOCATION_ID,
    contactId,
    startTime,
    endTime,
    title: record.ItemDescription || "Schedule Anyone Appointment",
    appointmentStatus: "confirmed",
    ignoreFreeSlotValidation: true,
    ignoreDateRange: true
  };

  const response = await axios.post(
    `${process.env.GHL_BASE_URL}/calendars/events/appointments`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-04-15",
        "Content-Type": "application/json"
      }
    }
  );

  return response.data;
}

async function updateAppointment(id, record, contactId, startTime, endTime) {
  await axios.put(
    `${process.env.GHL_BASE_URL}/calendars/events/appointments/${id}`,
    {
      calendarId: process.env.GHL_CALENDAR_ID,
      locationId: process.env.GHL_LOCATION_ID,
      contactId,
      startTime,
      endTime,
      title: record.ItemDescription,
      appointmentStatus: "confirmed",
      ignoreFreeSlotValidation: true,
      ignoreDateRange: true
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-04-15"
      }
    }
  );
}

function shouldSkipPastAppointment(startTime) {
  const now = DateTime.now().setZone("America/New_York");
  const appt = DateTime.fromISO(startTime);

  return appt < now;
}

async function syncOnce() {
  if (isSyncRunning) return;
  isSyncRunning = true;

  try {
    console.log("Starting sync...");

    const mapping = loadMapping();
    const { start, end } = getDateWindow();

    console.log(`Checking ${start} → ${end}`);

    const records = await getScheduleAnyoneData(start, end);

    for (const record of records) {
      const ticket = record.TicketNumber;

      if (!ticket) continue;

      const startTime = toISOWithOffset(record.StartDate);
      const endTime = toISOWithOffset(record.EndDate);

      if (!startTime || !endTime) continue;
      if (shouldSkipPastAppointment(startTime)) continue;

      try {
        const contact = await getOrCreateContact(record);
        if (!contact?.id) continue;

        if (!mapping[ticket]) {
          const created = await createAppointment(record, contact.id, startTime, endTime);

          const id = created?.id || created?.appointment?.id;

          mapping[ticket] = { ghlAppointmentId: id };
          console.log("Created:", ticket);
        } else {
          await updateAppointment(mapping[ticket].ghlAppointmentId, record, contact.id, startTime, endTime);
          console.log("Updated:", ticket);
        }

        saveMapping(mapping);
        await sleep(300);
      } catch (err) {
        console.log("Error:", ticket);
      }
    }

    console.log("Sync complete");
  } finally {
    isSyncRunning = false;
  }
}

async function main() {
  await syncOnce();

  cron.schedule(`*/${process.env.SYNC_MINUTES || 20} * * * *`, syncOnce);
}

main();