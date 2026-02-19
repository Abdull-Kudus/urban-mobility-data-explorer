/*
  dashboard.js
  ------------
  This is the "brain" of the dashboard. It does three main things:
    1. Fetches data from the Flask backend (http://localhost:5000)
    2. Draws charts using Chart.js
    3. Fills in the trips table and handles filters + pagination

  HOW TO READ THIS FILE:
  - Each section is clearly labelled with a big comment block.
  - Every function has a plain-English description of what it does.
  - Variables are named to be self-explanatory (e.g. "currentPage", "totalPages").

  IMPORTANT: The backend must be running before this page will show data.
    Start it with:  cd backend && python run.py
*/


/* ============================================================
   SECTION 1: CONFIGURATION
   Change the API base URL here if your backend runs on a different port.
   ============================================================ */

// The address where our Flask backend is listening.
// If you change the port in run.py, update this too.
const API_BASE = "http://localhost:5000";


/* ============================================================
   SECTION 2: STATE VARIABLES
   These variables remember the current state of the dashboard
   (which page we're on, what filters are active, etc.)
   ============================================================ */

// Which page of the trips table we're currently showing (starts at 1)
let currentPage = 1;

// How many rows to show per page
const ROWS_PER_PAGE = 50;

// Total number of pages (updated after each API call)
let totalPages = 1;

// The current filter values (updated when user clicks "Apply Filters")
let activeFilters = {};

// References to the Chart.js chart objects so we can destroy/redraw them
let chartHourly  = null;
let chartRevenue = null;
let chartFare    = null;
let chartZones   = null;

// Borough names collected from the revenue API (used to populate the dropdown)
let boroughList = [];


/* ============================================================
   SECTION 3: UTILITY FUNCTIONS
   Small helper functions used throughout the file.
   ============================================================ */

/**
 * fetchData(url)
 * --------------
 * Fetches JSON data from a URL and returns it.
 * If something goes wrong (network error, server error), it shows an error toast.
 *
 * @param {string} url - The full URL to fetch from.
 * @returns {object|null} - The parsed JSON data, or null if there was an error.
 */
async function fetchData(url) {
  try {
    // "await" pauses here until the server responds
    const response = await fetch(url);

    // If the server returned an error status (like 500), throw an error
    if (!response.ok) {
      throw new Error(`Server returned status ${response.status}`);
    }

    // Parse the response body as JSON and return it
    return await response.json();

  } catch (error) {
    // Something went wrong — show the error message to the user
    showToast(`Could not load data from ${url}. Is the backend running?`);
    console.error("Fetch error:", error);
    return null;
  }
}


/**
 * showToast(message)
 * ------------------
 * Shows a small error notification at the bottom-right of the screen.
 * It automatically disappears after 4 seconds.
 *
 * @param {string} message - The error message to display.
 */
function showToast(message) {
  const toast = document.getElementById("error-toast");
  const toastMsg = document.getElementById("toast-message");
  toastMsg.textContent = message;
  toast.classList.add("show");

  // Hide the toast after 4 seconds
  setTimeout(() => toast.classList.remove("show"), 4000);
}


/**
 * showLoading(id, show)
 * ---------------------
 * Shows or hides the spinning loading indicator inside a chart card.
 *
 * @param {string} id   - The ID of the loading overlay element.
 * @param {boolean} show - true = show spinner, false = hide it.
 */
function showLoading(id, show) {
  const el = document.getElementById(id);
  if (el) {
    // "active" class makes the overlay visible (see styles.css)
    el.classList.toggle("active", show);
  }
}


/**
 * formatNumber(num)
 * -----------------
 * Formats a large number with commas for readability.
 * Example: 1234567 → "1,234,567"
 *
 * @param {number} num - The number to format.
 * @returns {string} - The formatted string.
 */
function formatNumber(num) {
  if (num === null || num === undefined) return "—";
  // toLocaleString adds commas automatically
  return Number(num).toLocaleString("en-US");
}


/**
 * formatCurrency(num)
 * -------------------
 * Formats a number as a dollar amount.
 * Example: 13.5 → "$13.50"
 *
 * @param {number} num - The dollar amount.
 * @returns {string} - The formatted currency string.
 */
function formatCurrency(num) {
  if (num === null || num === undefined) return "—";
  return "$" + Number(num).toFixed(2);
}


/**
 * formatDecimal(num, places)
 * --------------------------
 * Rounds a number to a given number of decimal places.
 * Example: formatDecimal(14.8765, 1) → "14.9"
 *
 * @param {number} num    - The number to format.
 * @param {number} places - How many decimal places to show.
 * @returns {string} - The formatted string.
 */
function formatDecimal(num, places = 1) {
  if (num === null || num === undefined) return "—";
  return Number(num).toFixed(places);
}


/**
 * getBoroughBadgeClass(borough)
 * -----------------------------
 * Returns a CSS class name for the colored borough badge in the table.
 * Each borough gets a different color.
 *
 * @param {string} borough - The borough name.
 * @returns {string} - A CSS class name.
 */
function getBoroughBadgeClass(borough) {
  // Map borough names to color classes defined in styles.css
  const colorMap = {
    "Manhattan":    "badge",          // blue
    "Brooklyn":     "badge badge-green",
    "Queens":       "badge badge-orange",
    "Bronx":        "badge badge-purple",
    "Staten Island":"badge badge-red",
    "EWR":          "badge badge-red",
  };
  // Return the matching class, or default blue if not found
  return colorMap[borough] || "badge";
}


/* ============================================================
   SECTION 4: LIVE CLOCK
   Updates the clock in the navbar every second.
   ============================================================ */

/**
 * startClock()
 * ------------
 * Starts a timer that updates the clock display every second.
 */
function startClock() {
  function updateClock() {
    const now = new Date();

    // Format: HH:MM:SS  (padStart ensures two digits, e.g. "09" not "9")
    const hours   = String(now.getHours()).padStart(2, "0");
    const minutes = String(now.getMinutes()).padStart(2, "0");
    const seconds = String(now.getSeconds()).padStart(2, "0");

    const clockEl = document.getElementById("live-clock");
    if (clockEl) {
      clockEl.textContent = `${hours}:${minutes}:${seconds}`;
    }
  }

  // Run immediately, then every 1000 milliseconds (1 second)
  updateClock();
  setInterval(updateClock, 1000);
}


/* ============================================================
   SECTION 5: BACKEND STATUS CHECK
   Checks if the Flask backend is reachable and shows a green/red dot.
   ============================================================ */

/**
 * checkBackendStatus()
 * --------------------
 * Tries to reach the backend. Updates the status badge in the navbar.
 * Green dot = connected, Red dot = not reachable.
 */
async function checkBackendStatus() {
  const dot  = document.getElementById("status-dot");
  const text = document.getElementById("status-text");

  try {
    // Try fetching the hourly demand endpoint as a "ping"
    const response = await fetch(`${API_BASE}/api/analytics/hourly-demand`, {
      // "signal" lets us cancel the request if it takes too long
      signal: AbortSignal.timeout(5000)  // 5-second timeout
    });

    if (response.ok) {
      // Backend is reachable!
      dot.className  = "status-dot online";
      text.textContent = "Backend Online";
    } else {
      throw new Error("Bad response");
    }

  } catch (e) {
    // Backend is NOT reachable
    dot.className  = "status-dot offline";
    text.textContent = "Backend Offline";
  }
}


/* ============================================================
   SECTION 6: CHART DRAWING FUNCTIONS
   Each function fetches data from one API endpoint and draws a chart.
   ============================================================ */

/**
 * drawHourlyChart()
 * -----------------
 * Fetches trip counts per hour and draws a bar chart.
 * API: GET /api/analytics/hourly-demand
 */
async function drawHourlyChart() {
  showLoading("loading-hourly", true);

  const result = await fetchData(`${API_BASE}/api/analytics/hourly-demand`);

  showLoading("loading-hourly", false);

  // If the fetch failed or returned no data, stop here
  if (!result || !result.data) return;

  const data = result.data;

  // Pull out the hour numbers (0, 1, 2, … 23) for the X axis labels
  const labels = data.map(row => `${row.pickup_hour}:00`);

  // Pull out the trip counts for the bar heights
  const counts = data.map(row => row.trip_count);

  // If a chart already exists (from a previous load), destroy it first
  if (chartHourly) chartHourly.destroy();

  // Get the canvas element where Chart.js will draw
  const ctx = document.getElementById("chart-hourly").getContext("2d");

  // Create the bar chart
  chartHourly = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Number of Trips",
        data: counts,
        // Use a gradient of blue colors — darker bars for busier hours
        backgroundColor: counts.map((c, i) => {
          // Normalize: busiest hour = fully opaque, quietest = semi-transparent
          const max = Math.max(...counts);
          const opacity = 0.4 + 0.6 * (c / max);
          return `rgba(79, 142, 247, ${opacity})`;
        }),
        borderColor: "rgba(79, 142, 247, 0.8)",
        borderWidth: 1,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            // Custom tooltip: show "Hour 14:00 — 45,321 trips"
            label: (ctx) => ` ${formatNumber(ctx.raw)} trips`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#8892a4", font: { size: 11 } },
          grid:  { color: "rgba(42,49,71,0.5)" }
        },
        y: {
          ticks: {
            color: "#8892a4",
            font: { size: 11 },
            // Format Y axis numbers with commas (e.g. 50,000)
            callback: (val) => formatNumber(val)
          },
          grid: { color: "rgba(42,49,71,0.5)" }
        }
      }
    }
  });
}


/**
 * drawRevenueChart()
 * ------------------
 * Fetches revenue by zone, groups it by borough, and draws a horizontal bar chart.
 * API: GET /api/analytics/revenue-by-zone
 */
async function drawRevenueChart() {
  showLoading("loading-revenue", true);

  const result = await fetchData(`${API_BASE}/api/analytics/revenue-by-zone`);

  showLoading("loading-revenue", false);

  if (!result || !result.data) return;

  // --- Group zone data by borough ---
  // The API returns one row per zone. We want one bar per borough.
  // We'll add up the revenue for all zones in each borough.
  const boroughTotals = {};  // { "Manhattan": 1234567.89, "Brooklyn": 456789.12, ... }

  result.data.forEach(row => {
    const borough = row.borough_name;
    if (!boroughTotals[borough]) {
      boroughTotals[borough] = 0;
    }
    boroughTotals[borough] += row.total_revenue;
  });

  // Also collect borough names for the filter dropdown
  boroughList = Object.keys(boroughTotals);
  populateBoroughDropdown(boroughList);

  // Sort boroughs by revenue (highest first) for a cleaner chart
  // We do this manually without using .sort() on the values directly
  const boroughEntries = [];
  for (const name in boroughTotals) {
    boroughEntries.push({ name: name, revenue: boroughTotals[name] });
  }

  // Simple insertion sort (manual, no built-in sort)
  for (let i = 1; i < boroughEntries.length; i++) {
    const current = boroughEntries[i];
    let j = i - 1;
    while (j >= 0 && boroughEntries[j].revenue < current.revenue) {
      boroughEntries[j + 1] = boroughEntries[j];
      j--;
    }
    boroughEntries[j + 1] = current;
  }

  const labels   = boroughEntries.map(b => b.name);
  const revenues = boroughEntries.map(b => b.revenue);

  // Color palette for each borough bar
  const colors = [
    "rgba(79, 142, 247, 0.8)",   // blue
    "rgba(52, 211, 153, 0.8)",   // green
    "rgba(245, 158, 11, 0.8)",   // orange
    "rgba(167, 139, 250, 0.8)",  // purple
    "rgba(239, 68, 68, 0.8)",    // red
    "rgba(20, 184, 166, 0.8)",   // teal
  ];

  if (chartRevenue) chartRevenue.destroy();

  const ctx = document.getElementById("chart-revenue").getContext("2d");

  chartRevenue = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Total Revenue ($)",
        data: revenues,
        backgroundColor: colors.slice(0, labels.length),
        borderRadius: 6,
      }]
    },
    options: {
      indexAxis: "y",  // "y" makes it a horizontal bar chart
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => ` $${formatNumber(Math.round(ctx.raw))}`
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: "#8892a4",
            font: { size: 11 },
            callback: (val) => "$" + formatNumber(Math.round(val))
          },
          grid: { color: "rgba(42,49,71,0.5)" }
        },
        y: {
          ticks: { color: "#8892a4", font: { size: 12 } },
          grid:  { display: false }
        }
      }
    }
  });
}


/**
 * drawFareChart()
 * ---------------
 * Fetches average fare per distance bucket and draws a grouped bar chart.
 * API: GET /api/analytics/average-fare-per-mile
 */
async function drawFareChart() {
  showLoading("loading-fare", true);

  const result = await fetchData(`${API_BASE}/api/analytics/average-fare-per-mile`);

  showLoading("loading-fare", false);

  if (!result || !result.data) return;

  const data = result.data;

  const labels       = data.map(row => row.distance_bucket);
  const avgFares     = data.map(row => row.avg_fare);
  const avgFarePerMi = data.map(row => row.avg_fare_per_mile);

  if (chartFare) chartFare.destroy();

  const ctx = document.getElementById("chart-fare").getContext("2d");

  chartFare = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [
        {
          label: "Avg Total Fare ($)",
          data: avgFares,
          backgroundColor: "rgba(79, 142, 247, 0.8)",
          borderRadius: 4,
        },
        {
          label: "Avg Fare per Mile ($/mi)",
          data: avgFarePerMi,
          backgroundColor: "rgba(52, 211, 153, 0.8)",
          borderRadius: 4,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: true,
          labels: { color: "#8892a4", font: { size: 11 } }
        },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.dataset.label}: $${formatDecimal(ctx.raw, 2)}`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#8892a4", font: { size: 10 } },
          grid:  { color: "rgba(42,49,71,0.5)" }
        },
        y: {
          ticks: {
            color: "#8892a4",
            font: { size: 11 },
            callback: (val) => "$" + val
          },
          grid: { color: "rgba(42,49,71,0.5)" }
        }
      }
    }
  });
}


/**
 * drawTopZonesChart()
 * -------------------
 * Fetches the top 10 revenue zones (sorted by the backend's merge sort algorithm)
 * and draws a horizontal bar chart.
 * API: GET /api/analytics/top-revenue-zones?n=10
 */
async function drawTopZonesChart() {
  showLoading("loading-zones", true);

  const result = await fetchData(`${API_BASE}/api/analytics/top-revenue-zones?n=10`);

  showLoading("loading-zones", false);

  if (!result || !result.data) return;

  const data = result.data;

  // Shorten long zone names so they fit on the chart
  const labels   = data.map(row => row.zone_name.length > 22
    ? row.zone_name.substring(0, 22) + "…"
    : row.zone_name
  );
  const revenues = data.map(row => row.total_revenue);

  if (chartZones) chartZones.destroy();

  const ctx = document.getElementById("chart-zones").getContext("2d");

  chartZones = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Total Revenue ($)",
        data: revenues,
        // Gold gradient effect: top zone is brightest
        backgroundColor: revenues.map((r, i) => {
          const max = revenues[0];  // already sorted descending
          const opacity = 0.5 + 0.5 * (r / max);
          return `rgba(245, 158, 11, ${opacity})`;
        }),
        borderRadius: 4,
      }]
    },
    options: {
      indexAxis: "y",  // horizontal bars
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => ` $${formatNumber(Math.round(ctx.raw))}`
          }
        }
      },
      scales: {
        x: {
          ticks: {
            color: "#8892a4",
            font: { size: 11 },
            callback: (val) => "$" + formatNumber(Math.round(val))
          },
          grid: { color: "rgba(42,49,71,0.5)" }
        },
        y: {
          ticks: { color: "#8892a4", font: { size: 10 } },
          grid:  { display: false }
        }
      }
    }
  });
}


/* ============================================================
   SECTION 7: TRIPS TABLE
   Loads trip records from the API and fills in the HTML table.
   ============================================================ */

/**
 * loadTrips(filters, page)
 * ------------------------
 * Fetches a page of trip records from the API and renders them in the table.
 *
 * @param {object} filters - An object with filter values (start_date, end_date, etc.)
 * @param {number} page    - Which page number to load (1-indexed).
 */
async function loadTrips(filters = {}, page = 1) {
  // Show the loading spinner over the table
  showLoading("loading-table", true);

  // --- Build the URL with query parameters ---
  // Example: /api/trips?start_date=2019-01-01&end_date=2019-01-31&page=1&limit=50
  const params = new URLSearchParams();
  params.set("page",  page);
  params.set("limit", ROWS_PER_PAGE);

  // Add each filter value to the URL (only if it's not empty)
  if (filters.start_date)   params.set("start_date",   filters.start_date + " 00:00:00");
  if (filters.end_date)     params.set("end_date",     filters.end_date   + " 23:59:59");
  if (filters.pickup_zone)  params.set("pickup_zone",  filters.pickup_zone);
  if (filters.min_fare)     params.set("min_fare",     filters.min_fare);
  if (filters.max_fare)     params.set("max_fare",     filters.max_fare);
  if (filters.min_distance) params.set("min_distance", filters.min_distance);

  const url = `${API_BASE}/api/trips?${params.toString()}`;
  const result = await fetchData(url);

  showLoading("loading-table", false);

  if (!result) return;

  // Update pagination state
  currentPage = result.page || 1;
  totalPages  = result.total_pages || 1;

  // Update the summary text above the table
  const countEl = document.getElementById("table-count");
  if (countEl) {
    countEl.textContent = `${formatNumber(result.total_count)} trips found`;
  }

  // Update the KPI summary cards using the first page of data
  updateSummaryCards(result);

  // Fill in the table rows
  renderTableRows(result.data || []);

  // Update the pagination controls
  updatePagination();
}


/**
 * renderTableRows(trips)
 * ----------------------
 * Takes an array of trip objects and creates HTML table rows for each one.
 *
 * @param {Array} trips - Array of trip record objects from the API.
 */
function renderTableRows(trips) {
  const tbody = document.getElementById("trips-tbody");

  // If there are no trips, show an empty state message
  if (!trips || trips.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="10">
          <div class="empty-state">
            <div class="empty-icon">🔍</div>
            <p>No trips match your current filters. Try adjusting the date range or removing some filters.</p>
          </div>
        </td>
      </tr>
    `;
    return;
  }

  // Build the HTML for all rows at once (faster than adding one row at a time)
  let html = "";

  trips.forEach(trip => {
    // Format the pickup time: "2019-01-03 14:22:10" → "Jan 3, 2019 14:22"
    const pickupTime = trip.pickup_datetime
      ? trip.pickup_datetime.replace("T", " ").substring(0, 16)
      : "—";

    // Get the colored badge class for the borough
    const badgeClass = getBoroughBadgeClass(trip.pickup_borough);

    html += `
      <tr>
        <td>${pickupTime}</td>
        <td>${trip.pickup_zone  || "—"}</td>
        <td>${trip.dropoff_zone || "—"}</td>
        <td><span class="${badgeClass}">${trip.pickup_borough || "—"}</span></td>
        <td>${formatDecimal(trip.trip_distance, 2)}</td>
        <td>${formatCurrency(trip.fare_amount)}</td>
        <td>${formatCurrency(trip.total_amount)}</td>
        <td>${formatDecimal(trip.trip_duration_minutes, 1)}</td>
        <td>${formatDecimal(trip.avg_speed_mph, 1)}</td>
        <td>${trip.passenger_count || "—"}</td>
      </tr>
    `;
  });

  tbody.innerHTML = html;
}


/**
 * updateSummaryCards(result)
 * --------------------------
 * Calculates and displays the 4 KPI summary numbers at the top of the page.
 * We calculate averages from the current page of data as an approximation.
 *
 * @param {object} result - The API response object from /api/trips.
 */
function updateSummaryCards(result) {
  // Total trips: use the total_count from the API (not just this page)
  const totalEl = document.getElementById("stat-total-trips");
  if (totalEl) totalEl.textContent = formatNumber(result.total_count);

  // For averages, we calculate from the current page of data
  const trips = result.data || [];
  if (trips.length === 0) return;

  // Calculate averages manually (no built-in reduce or map for the math)
  let sumFare = 0, sumDist = 0, sumSpeed = 0, count = 0;

  for (let i = 0; i < trips.length; i++) {
    const t = trips[i];
    if (t.total_amount    !== null) sumFare  += t.total_amount;
    if (t.trip_distance   !== null) sumDist  += t.trip_distance;
    if (t.avg_speed_mph   !== null) sumSpeed += t.avg_speed_mph;
    count++;
  }

  if (count > 0) {
    const avgFare  = sumFare  / count;
    const avgDist  = sumDist  / count;
    const avgSpeed = sumSpeed / count;

    const fareEl  = document.getElementById("stat-avg-fare");
    const distEl  = document.getElementById("stat-avg-distance");
    const speedEl = document.getElementById("stat-avg-speed");

    if (fareEl)  fareEl.textContent  = formatCurrency(avgFare);
    if (distEl)  distEl.textContent  = formatDecimal(avgDist, 2) + " mi";
    if (speedEl) speedEl.textContent = formatDecimal(avgSpeed, 1) + " mph";
  }
}


/* ============================================================
   SECTION 8: PAGINATION
   Controls for moving between pages of the trips table.
   ============================================================ */

/**
 * updatePagination()
 * ------------------
 * Updates the "Page X of Y" text and enables/disables the Prev/Next buttons.
 */
function updatePagination() {
  const prevBtn    = document.getElementById("btn-prev");
  const nextBtn    = document.getElementById("btn-next");
  const pageDisplay = document.getElementById("page-display");
  const pageInfo   = document.getElementById("pagination-info");

  if (pageDisplay) pageDisplay.textContent = `Page ${currentPage} of ${totalPages}`;
  if (pageInfo)    pageInfo.textContent    = `Showing page ${currentPage} of ${totalPages}`;

  // Disable "Previous" button if we're on the first page
  if (prevBtn) prevBtn.disabled = (currentPage <= 1);

  // Disable "Next" button if we're on the last page
  if (nextBtn) nextBtn.disabled = (currentPage >= totalPages);
}


/**
 * changePage(direction)
 * ---------------------
 * Moves to the next or previous page of the trips table.
 * Called by the Prev/Next buttons in the HTML.
 *
 * @param {number} direction - +1 to go forward, -1 to go back.
 */
function changePage(direction) {
  const newPage = currentPage + direction;

  // Make sure we don't go below page 1 or above the last page
  if (newPage < 1 || newPage > totalPages) return;

  currentPage = newPage;

  // Reload the table with the new page number
  loadTrips(activeFilters, currentPage);

  // Scroll back to the top of the table so the user can see the new rows
  document.querySelector(".table-section").scrollIntoView({ behavior: "smooth" });
}


/* ============================================================
   SECTION 9: FILTER CONTROLS
   Reads the filter form values and reloads the trips table.
   ============================================================ */

/**
 * populateBoroughDropdown(boroughs)
 * ---------------------------------
 * Fills the borough dropdown with options from the data.
 * Called after the revenue-by-zone data is loaded.
 *
 * @param {string[]} boroughs - Array of borough name strings.
 */
function populateBoroughDropdown(boroughs) {
  const select = document.getElementById("filter-borough");
  if (!select) return;

  // Keep the first "All Boroughs" option, remove any old options after it
  while (select.options.length > 1) {
    select.remove(1);
  }

  // Add one option per borough
  boroughs.forEach(borough => {
    const option = document.createElement("option");
    option.value = borough;       // what gets sent to the API
    option.textContent = borough; // what the user sees
    select.appendChild(option);
  });
}


/**
 * applyFilters()
 * --------------
 * Reads all the filter input values and reloads the trips table.
 * Called when the user clicks the "Apply Filters" button.
 */
function applyFilters() {
  // Read each filter input value from the HTML form
  const startDate   = document.getElementById("filter-start-date").value;
  const endDate     = document.getElementById("filter-end-date").value;
  const borough     = document.getElementById("filter-borough").value;
  const minFare     = document.getElementById("filter-min-fare").value;
  const maxFare     = document.getElementById("filter-max-fare").value;
  const minDistance = document.getElementById("filter-min-distance").value;

  // Validate: end date must be after start date
  if (startDate && endDate && startDate > endDate) {
    showToast("End date must be after start date.");
    return;
  }

  // Validate: max fare must be greater than min fare
  if (minFare && maxFare && parseFloat(minFare) > parseFloat(maxFare)) {
    showToast("Max fare must be greater than min fare.");
    return;
  }

  // Save the active filters so pagination can use them
  activeFilters = {
    start_date:   startDate,
    end_date:     endDate,
    pickup_zone:  borough,     // borough name is used as a zone name filter
    min_fare:     minFare,
    max_fare:     maxFare,
    min_distance: minDistance,
  };

  // Reset to page 1 when filters change
  currentPage = 1;

  // Reload the table with the new filters
  loadTrips(activeFilters, 1);
}


/**
 * resetFilters()
 * --------------
 * Clears all filter inputs and reloads the table with no filters.
 * Called when the user clicks the "Reset" button.
 */
function resetFilters() {
  // Clear each input field
  document.getElementById("filter-start-date").value   = "2019-01-01";
  document.getElementById("filter-end-date").value     = "2019-01-31";
  document.getElementById("filter-borough").value      = "";
  document.getElementById("filter-min-fare").value     = "";
  document.getElementById("filter-max-fare").value     = "";
  document.getElementById("filter-min-distance").value = "";

  // Clear the active filters
  activeFilters = {};
  currentPage   = 1;

  // Reload the table with no filters
  loadTrips({}, 1);
}


/* ============================================================
   SECTION 10: INITIALIZATION
   This runs when the page first loads. It starts everything.
   ============================================================ */

/**
 * init()
 * ------
 * The main startup function. Called once when the page loads.
 * It starts the clock, checks the backend, and loads all data.
 */
async function init() {
  // 1. Start the live clock in the navbar
  startClock();

  // 2. Check if the backend is reachable
  await checkBackendStatus();

  // 3. Load all four charts at the same time (in parallel, faster than one by one)
  //    Promise.all() waits for ALL of them to finish before continuing.
  await Promise.all([
    drawHourlyChart(),
    drawRevenueChart(),  // also populates the borough dropdown
    drawFareChart(),
    drawTopZonesChart(),
  ]);

  // 4. Load the first page of the trips table with default filters
  await loadTrips({
    start_date: "2019-01-01",
    end_date:   "2019-01-31",
  }, 1);

  // 5. Set the default filter values to match what we just loaded
  activeFilters = {
    start_date: "2019-01-01",
    end_date:   "2019-01-31",
  };
}

// --- Run init() as soon as the page has finished loading ---
// "DOMContentLoaded" fires when the HTML is ready but before images load.
document.addEventListener("DOMContentLoaded", init);
