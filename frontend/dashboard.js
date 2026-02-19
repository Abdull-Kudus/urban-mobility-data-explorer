// Backend URL - change this if Flask runs on a different port
const API_BASE = "http://localhost:5000";

let currentPage = 1;
const ROWS_PER_PAGE = 50;
let totalPages = 1;
let activeFilters = {};
let chartHourly = null;
let chartRevenue = null;
let chartFare = null;
let chartZones = null;

// Borough names loaded from the API
let boroughList = [];


// Fetch JSON from a URL - returns null if something goes wrong
async function fetchData(url) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Server returned status ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    showToast(`Could not load data. Is the backend running?`);
    console.error("Fetch error:", error);
    return null;
  }
}


// for showing error messages
function showToast(message) {
  const toast = document.getElementById("error-toast");
  const toastMsg = document.getElementById("toast-message");
  toastMsg.textContent = message;
  toast.classList.add("show");
  setTimeout(() => toast.classList.remove("show"), 4000);
}


function showLoading(id, show) {
  const el = document.getElementById(id);
  if (el) {
    el.classList.toggle("active", show);
  }
}


function formatNumber(num) {
  if (num === null || num === undefined) return "—";
  return Number(num).toLocaleString("en-US");
}


function formatCurrency(num) {
  if (num === null || num === undefined) return "—";
  return "$" + Number(num).toFixed(2);
}


function formatDecimal(num, places = 1) {
  if (num === null || num === undefined) return "—";
  return Number(num).toFixed(places);
}


function getBoroughBadgeClass(borough) {
  const colorMap = {
    "Manhattan": "badge",
    "Brooklyn": "badge badge-green",
    "Queens": "badge badge-orange",
    "Bronx": "badge badge-purple",
    "Staten Island": "badge badge-red",
    "EWR": "badge badge-red",
  };
  return colorMap[borough] || "badge";
}


function startClock() {
  function updateClock() {
    const now = new Date();
    const hours = String(now.getHours()).padStart(2, "0");
    const minutes = String(now.getMinutes()).padStart(2, "0");
    const seconds = String(now.getSeconds()).padStart(2, "0");
    const clockEl = document.getElementById("live-clock");
    if (clockEl) {
      clockEl.textContent = `${hours}:${minutes}:${seconds}`;
    }
  }
  updateClock();
  setInterval(updateClock, 1000);
}


async function checkBackendStatus() {
  const dot = document.getElementById("status-dot");
  const text = document.getElementById("status-text");
  try {
    const response = await fetch(`${API_BASE}/api/analytics/hourly-demand`, {
      signal: AbortSignal.timeout(5000)
    });
    if (response.ok) {
      dot.className = "status-dot online";
      text.textContent = "Backend Online";
    } else {
      throw new Error("Bad response");
    }
  } catch (e) {
    dot.className = "status-dot offline";
    text.textContent = "Backend Offline";
  }
}


async function drawHourlyChart() {
  showLoading("loading-hourly", true);
  const result = await fetchData(`${API_BASE}/api/analytics/hourly-demand`);
  showLoading("loading-hourly", false);

  if (!result || !result.data) return;

  const data = result.data;
  const labels = data.map(row => `${row.pickup_hour}:00`);
  const counts = data.map(row => row.trip_count);

  if (chartHourly) chartHourly.destroy();

  const ctx = document.getElementById("chart-hourly").getContext("2d");

  chartHourly = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Number of Trips",
        data: counts,
        backgroundColor: counts.map((c) => {
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
            label: (ctx) => ` ${formatNumber(ctx.raw)} trips`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#6b7280", font: { size: 11 } },
          grid: { color: "rgba(229,231,235,0.8)" }
        },
        y: {
          ticks: {
            color: "#6b7280",
            font: { size: 11 },
            callback: (val) => formatNumber(val)
          },
          grid: { color: "rgba(229,231,235,0.8)" }
        }
      }
    }
  });
}


async function drawRevenueChart() {
  showLoading("loading-revenue", true);
  const result = await fetchData(`${API_BASE}/api/analytics/revenue-by-zone`);
  showLoading("loading-revenue", false);

  if (!result || !result.data) return;

  const boroughTotals = {};
  result.data.forEach(row => {
    const borough = row.borough_name;
    if (!boroughTotals[borough]) {
      boroughTotals[borough] = 0;
    }
    boroughTotals[borough] += row.total_revenue;
  });

  boroughList = Object.keys(boroughTotals);
  populateBoroughDropdown(boroughList);

  // sorting boroughs by revenue using insertion sort
  const boroughEntries = [];
  for (const name in boroughTotals) {
    boroughEntries.push({ name: name, revenue: boroughTotals[name] });
  }

  for (let i = 1; i < boroughEntries.length; i++) {
    const current = boroughEntries[i];
    let j = i - 1;
    while (j >= 0 && boroughEntries[j].revenue < current.revenue) {
      boroughEntries[j + 1] = boroughEntries[j];
      j--;
    }
    boroughEntries[j + 1] = current;
  }

  const labels = boroughEntries.map(b => b.name);
  const revenues = boroughEntries.map(b => b.revenue);

  const colors = [
    "rgba(79, 142, 247, 0.8)",
    "rgba(52, 211, 153, 0.8)",
    "rgba(245, 158, 11, 0.8)",
    "rgba(167, 139, 250, 0.8)",
    "rgba(239, 68, 68, 0.8)",
    "rgba(20, 184, 166, 0.8)",
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
      indexAxis: "y",
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
            color: "#6b7280",
            font: { size: 11 },
            callback: (val) => "$" + formatNumber(Math.round(val))
          },
          grid: { color: "rgba(229,231,235,0.8)" }
        },
        y: {
          ticks: { color: "#6b7280", font: { size: 12 } },
          grid: { display: false }
        }
      }
    }
  });
}


async function drawFareChart() {
  showLoading("loading-fare", true);
  const result = await fetchData(`${API_BASE}/api/analytics/average-fare-per-mile`);
  showLoading("loading-fare", false);

  if (!result || !result.data) return;

  const data = result.data;
  const labels = data.map(row => row.distance_bucket);
  const avgFares = data.map(row => row.avg_fare);
  const avgPerMile = data.map(row => row.avg_fare_per_mile);

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
          data: avgPerMile,
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
          labels: { color: "#6b7280", font: { size: 11 } }
        },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.dataset.label}: $${formatDecimal(ctx.raw, 2)}`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#6b7280", font: { size: 10 } },
          grid: { color: "rgba(229,231,235,0.8)" }
        },
        y: {
          ticks: {
            color: "#6b7280",
            font: { size: 11 },
            callback: (val) => "$" + val
          },
          grid: { color: "rgba(229,231,235,0.8)" }
        }
      }
    }
  });
}


async function drawTopZonesChart() {
  showLoading("loading-zones", true);
  const result = await fetchData(`${API_BASE}/api/analytics/top-revenue-zones?n=10`);
  showLoading("loading-zones", false);

  if (!result || !result.data) return;

  const data = result.data;

  const labels = data.map(row => row.zone_name.length > 22
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
        backgroundColor: revenues.map((r) => {
          const max = revenues[0];
          const opacity = 0.5 + 0.5 * (r / max);
          return `rgba(245, 158, 11, ${opacity})`;
        }),
        borderRadius: 4,
      }]
    },
    options: {
      indexAxis: "y",
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
            color: "#6b7280",
            font: { size: 11 },
            callback: (val) => "$" + formatNumber(Math.round(val))
          },
          grid: { color: "rgba(229,231,235,0.8)" }
        },
        y: {
          ticks: { color: "#6b7280", font: { size: 10 } },
          grid: { display: false }
        }
      }
    }
  });
}


async function loadTrips(filters = {}, page = 1) {
  showLoading("loading-table", true);

  const params = new URLSearchParams();
  params.set("page", page);
  params.set("limit", ROWS_PER_PAGE);

  if (filters.start_date) params.set("start_date", filters.start_date + " 00:00:00");
  if (filters.end_date) params.set("end_date", filters.end_date + " 23:59:59");
  if (filters.pickup_zone) params.set("pickup_zone", filters.pickup_zone);
  if (filters.min_fare) params.set("min_fare", filters.min_fare);
  if (filters.max_fare) params.set("max_fare", filters.max_fare);
  if (filters.min_distance) params.set("min_distance", filters.min_distance);

  const url = `${API_BASE}/api/trips?${params.toString()}`;
  const result = await fetchData(url);

  showLoading("loading-table", false);

  if (!result) return;

  currentPage = result.page || 1;
  totalPages = result.total_pages || 1;

  const countEl = document.getElementById("table-count");
  if (countEl) {
    countEl.textContent = `${formatNumber(result.total_count)} trips found`;
  }

  updateSummaryCards(result);
  renderTableRows(result.data || []);
  updatePagination();
}


function renderTableRows(trips) {
  const tbody = document.getElementById("trips-tbody");

  if (!trips || trips.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="10">
          <div class="empty-state">
            <p>No trips match your current filters. Try adjusting the date range or removing some filters.</p>
          </div>
        </td>
      </tr>
    `;
    return;
  }

  let html = "";

  trips.forEach(trip => {
    const pickupTime = trip.pickup_datetime
      ? trip.pickup_datetime.replace("T", " ").substring(0, 16)
      : "—";
    const badgeClass = getBoroughBadgeClass(trip.pickup_borough);

    html += `
      <tr>
        <td>${pickupTime}</td>
        <td>${trip.pickup_zone || "—"}</td>
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


function updateSummaryCards(result) {
  const totalEl = document.getElementById("stat-total-trips");
  if (totalEl) totalEl.textContent = formatNumber(result.total_count);

  const trips = result.data || [];
  if (trips.length === 0) return;

  let sumFare = 0, sumDist = 0, sumSpeed = 0, count = 0;

  for (let i = 0; i < trips.length; i++) {
    const t = trips[i];
    if (t.total_amount !== null) sumFare += t.total_amount;
    if (t.trip_distance !== null) sumDist += t.trip_distance;
    if (t.avg_speed_mph !== null) sumSpeed += t.avg_speed_mph;
    count++;
  }

  if (count > 0) {
    const avgFare = sumFare / count;
    const avgDist = sumDist / count;
    const avgSpeed = sumSpeed / count;

    const fareEl = document.getElementById("stat-avg-fare");
    const distEl = document.getElementById("stat-avg-distance");
    const speedEl = document.getElementById("stat-avg-speed");

    if (fareEl) fareEl.textContent = formatCurrency(avgFare);
    if (distEl) distEl.textContent = formatDecimal(avgDist, 2) + " mi";
    if (speedEl) speedEl.textContent = formatDecimal(avgSpeed, 1) + " mph";
  }
}


function updatePagination() {
  const prevBtn = document.getElementById("btn-prev");
  const nextBtn = document.getElementById("btn-next");
  const pageDisplay = document.getElementById("page-display");
  const pageInfo = document.getElementById("pagination-info");

  if (pageDisplay) pageDisplay.textContent = `Page ${currentPage} of ${totalPages}`;
  if (pageInfo) pageInfo.textContent = `Showing page ${currentPage} of ${totalPages}`;

  if (prevBtn) prevBtn.disabled = (currentPage <= 1);
  if (nextBtn) nextBtn.disabled = (currentPage >= totalPages);
}


function changePage(direction) {
  const newPage = currentPage + direction;
  if (newPage < 1 || newPage > totalPages) return;
  currentPage = newPage;
  loadTrips(activeFilters, currentPage);
  document.querySelector(".table-section").scrollIntoView({ behavior: "smooth" });
}


function populateBoroughDropdown(boroughs) {
  const select = document.getElementById("filter-borough");
  if (!select) return;

  while (select.options.length > 1) {
    select.remove(1);
  }

  boroughs.forEach(borough => {
    const option = document.createElement("option");
    option.value = borough;
    option.textContent = borough;
    select.appendChild(option);
  });
}


function applyFilters() {
  const startDate = document.getElementById("filter-start-date").value;
  const endDate = document.getElementById("filter-end-date").value;
  const borough = document.getElementById("filter-borough").value;
  const minFare = document.getElementById("filter-min-fare").value;
  const maxFare = document.getElementById("filter-max-fare").value;
  const minDistance = document.getElementById("filter-min-distance").value;

  if (startDate && endDate && startDate > endDate) {
    showToast("End date must be after start date.");
    return;
  }

  if (minFare && maxFare && parseFloat(minFare) > parseFloat(maxFare)) {
    showToast("Max fare must be greater than min fare.");
    return;
  }

  activeFilters = {
    start_date: startDate,
    end_date: endDate,
    pickup_zone: borough,
    min_fare: minFare,
    max_fare: maxFare,
    min_distance: minDistance,
  };

  currentPage = 1;
  loadTrips(activeFilters, 1);
}


function resetFilters() {
  document.getElementById("filter-start-date").value = "2019-01-01";
  document.getElementById("filter-end-date").value = "2019-01-31";
  document.getElementById("filter-borough").value = "";
  document.getElementById("filter-min-fare").value = "";
  document.getElementById("filter-max-fare").value = "";
  document.getElementById("filter-min-distance").value = "";

  activeFilters = {};
  currentPage = 1;
  loadTrips({}, 1);
}


// start everything when the page has loaded
async function init() {
  startClock();
  await checkBackendStatus();

  // load all charts at the same time to save time
  await Promise.all([
    drawHourlyChart(),
    drawRevenueChart(),
    drawFareChart(),
    drawTopZonesChart(),
  ]);

  // load the first page of trip records with the default date range
  await loadTrips({
    start_date: "2019-01-01",
    end_date: "2019-01-31",
  }, 1);

  activeFilters = {
    start_date: "2019-01-01",
    end_date: "2019-01-31",
  };
}

document.addEventListener("DOMContentLoaded", init);
