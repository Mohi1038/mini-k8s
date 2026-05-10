const storageKey = "mini-k8ts-dashboard-settings";

const state = {
  cluster: { nodes: [], tasks: [] },
  jobs: [],
  namespaces: [],
  services: [],
  serviceStatus: {},
  insights: [],
  filters: {
    namespace: "",
    status: "",
    search: "",
  },
  selected: null,
  loading: false,
};

const els = {
  baseURL: document.querySelector("#base-url"),
  connectButton: document.querySelector("#connect-button"),
  refreshButton: document.querySelector("#refresh-button"),
  clearFiltersButton: document.querySelector("#clear-filters"),
  connectionStatus: document.querySelector("#connection-status"),
  refreshNote: document.querySelector("#refresh-note"),
  namespaceFilter: document.querySelector("#namespace-filter"),
  statusFilter: document.querySelector("#status-filter"),
  searchFilter: document.querySelector("#search-filter"),
  insightsList: document.querySelector("#insights-list"),
  namespacesList: document.querySelector("#namespaces-list"),
  nodesList: document.querySelector("#nodes-list"),
  servicesList: document.querySelector("#services-list"),
  jobsList: document.querySelector("#jobs-list"),
  detailPanel: document.querySelector("#detail-panel"),
  tasksBody: document.querySelector("#tasks-body"),
  statNodes: document.querySelector("#stat-nodes"),
  statReady: document.querySelector("#stat-ready"),
  statRunning: document.querySelector("#stat-running"),
  statPending: document.querySelector("#stat-pending"),
  statJobs: document.querySelector("#stat-jobs"),
  statServices: document.querySelector("#stat-services"),
  emptyState: document.querySelector("#empty-state-template"),
};

function loadSettings() {
  try {
    return JSON.parse(window.localStorage.getItem(storageKey) || "{}");
  } catch {
    return {};
  }
}

function saveSettings(settings) {
  window.localStorage.setItem(storageKey, JSON.stringify(settings));
}

function getSettings() {
  return {
    baseURL: (els.baseURL.value || "http://localhost:8080").trim().replace(/\/$/, ""),
  };
}

async function fetchJSON(path) {
  const settings = getSettings();
  const response = await fetch(`${settings.baseURL}${path}`);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }
  return response.json();
}

async function fetchServiceStatus(service) {
  if (!service) {
    return null;
  }
  const key = `${service.namespace}/${service.name}`;
  if (state.serviceStatus[key]) {
    return state.serviceStatus[key];
  }
  const payload = await fetchJSON(`/services/${encodeURIComponent(service.name)}/status?namespace=${encodeURIComponent(service.namespace)}`);
  state.serviceStatus[key] = payload;
  return payload;
}

function statusChip(value) {
  const chip = document.createElement("span");
  chip.className = `chip ${String(value || "info").toLowerCase()}`;
  chip.textContent = value || "unknown";
  return chip;
}

function emptyState(message = "Nothing to show yet.") {
  const node = els.emptyState.content.firstElementChild.cloneNode(true);
  node.textContent = message;
  return node;
}

function renderList(container, items, builder, emptyMessage) {
  container.replaceChildren();
  if (!items.length) {
    container.appendChild(emptyState(emptyMessage));
    return;
  }
  items.forEach((item) => container.appendChild(builder(item)));
}

function buildCard({ title, badgeValue, lines, onClick, active = false }) {
  const card = document.createElement("article");
  card.className = `item-card${active ? " active" : ""}`;
  if (onClick) {
    card.addEventListener("click", onClick);
  }

  const head = document.createElement("div");
  head.className = "item-head";

  const label = document.createElement("div");
  label.className = "item-title";
  label.textContent = title;
  head.appendChild(label);
  if (badgeValue) {
    head.appendChild(statusChip(badgeValue));
  }

  const meta = document.createElement("div");
  meta.className = "meta";
  lines.forEach((line) => {
    const row = document.createElement("div");
    row.textContent = line;
    meta.appendChild(row);
  });

  card.append(head, meta);
  return card;
}

function selectedKey() {
  if (!state.selected) {
    return "";
  }
  return `${state.selected.type}:${state.selected.id}`;
}

function isSelected(type, id) {
  return selectedKey() === `${type}:${id}`;
}

function setSelected(type, payload) {
  state.selected = {
    type,
    id: type === "service" ? `${payload.namespace}/${payload.name}` : payload.id || payload.name,
    payload,
  };
  renderAll();
  if (type === "service") {
    fetchServiceStatus(payload)
      .then(() => renderAll())
      .catch((error) => setStatus(`Failed to load service detail: ${error.message}`, true));
  }
}

function filterTasks(tasks) {
  const search = state.filters.search.trim().toLowerCase();
  return tasks.filter((task) => {
    if (state.filters.namespace && (task.namespace || "default") !== state.filters.namespace) {
      return false;
    }
    if (state.filters.status && task.status !== state.filters.status) {
      return false;
    }
    if (!search) {
      return true;
    }
    return [task.id, task.image, task.node_id, task.job_id, task.namespace]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(search));
  });
}

function filterJobs(jobs) {
  const search = state.filters.search.trim().toLowerCase();
  return jobs.filter((job) => {
    if (state.filters.namespace && (job.namespace || "default") !== state.filters.namespace) {
      return false;
    }
    if (!search) {
      return true;
    }
    return [job.id, job.image, job.namespace].some((value) => String(value).toLowerCase().includes(search));
  });
}

function filterServices(services) {
  const search = state.filters.search.trim().toLowerCase();
  return services.filter((service) => {
    if (state.filters.namespace && (service.namespace || "default") !== state.filters.namespace) {
      return false;
    }
    if (!search) {
      return true;
    }
    return [service.name, service.namespace, service.selector_job_id]
      .some((value) => String(value).toLowerCase().includes(search));
  });
}

function filterNodes(nodes) {
  const search = state.filters.search.trim().toLowerCase();
  if (!search) {
    return nodes;
  }
  return nodes.filter((node) => String(node.id).toLowerCase().includes(search));
}

function renderStats(nodes, tasks, jobs, services) {
  const ready = nodes.filter((node) => node.status === "READY").length;
  const running = tasks.filter((task) => task.status === "RUNNING").length;
  const pending = tasks.filter((task) => task.status === "PENDING").length;

  els.statNodes.textContent = nodes.length;
  els.statReady.textContent = ready;
  els.statRunning.textContent = running;
  els.statPending.textContent = pending;
  els.statJobs.textContent = jobs.length;
  els.statServices.textContent = services.length;
}

function renderNodes(nodes, allTasks) {
  renderList(
    els.nodesList,
    nodes,
    (node) => {
      const runningTasks = allTasks.filter((task) => task.node_id === node.id && task.status === "RUNNING").length;
      return buildCard({
        title: node.id,
        badgeValue: node.status,
        active: isSelected("node", node.id),
        lines: [
          `CPU ${node.used_cpu}/${node.total_cpu}`,
          `Memory ${node.used_memory}/${node.total_memory} MB`,
          `Reliability ${Number(node.reliability || 0).toFixed(2)}`,
          `Running tasks ${runningTasks}`,
        ],
        onClick: () => setSelected("node", node),
      });
    },
    "No nodes match the current filters.",
  );
}

function renderJobs(jobs) {
  renderList(
    els.jobsList,
    jobs,
    (job) =>
      buildCard({
        title: job.id,
        badgeValue: job.status,
        active: isSelected("job", job.id),
        lines: [
          `Namespace ${job.namespace || "default"}`,
          `Image ${job.image}`,
          `Replicas ${job.replicas}`,
          `Secrets ${job.secret_refs?.length || 0} | Volumes ${job.volumes?.length || 0}`,
        ],
        onClick: () => setSelected("job", job),
      }),
    "No jobs match the current filters.",
  );
}

function renderServices(services) {
  renderList(
    els.servicesList,
    services,
    (service) => {
      const summary = service.summary || {};
      const badge =
        Number(summary.unhealthy_endpoints || 0) > 0
          ? "degraded"
          : Number(summary.healthy_endpoints || 0) > 0
            ? "healthy"
            : "info";
      return buildCard({
        title: service.name,
        badgeValue: badge,
        active: isSelected("service", `${service.namespace}/${service.name}`),
        lines: [
          `Namespace ${service.namespace}`,
          `Job ${service.selector_job_id}`,
          `Port ${service.port}`,
          `Healthy ${summary.healthy_endpoints || 0} | Degraded ${summary.degraded_endpoints || 0}`,
        ],
        onClick: () => setSelected("service", service),
      });
    },
    "No services match the current filters.",
  );
}

function renderNamespaces(namespaces) {
  renderList(
    els.namespacesList,
    namespaces,
    (ns) =>
      buildCard({
        title: ns.name,
        badgeValue: "info",
        active: isSelected("namespace", ns.name),
        lines: [
          `CPU quota ${ns.cpu_quota || "unbounded"}`,
          `Memory quota ${ns.memory_quota ? `${ns.memory_quota} MB` : "unbounded"}`,
          `Task quota ${ns.task_quota || "unbounded"}`,
        ],
        onClick: () => setSelected("namespace", ns),
      }),
    "No namespaces found.",
  );
}

function renderInsights(insights) {
  renderList(
    els.insightsList,
    insights,
    (insight) =>
      buildCard({
        title: insight.message,
        badgeValue: insight.severity,
        lines: [insight.suggestion || "No suggestion available"],
      }),
    "No insights right now. The cluster looks calm.",
  );
}

function renderTasks(tasks) {
  els.tasksBody.replaceChildren();
  if (!tasks.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 8;
    cell.className = "empty-state";
    cell.textContent = "No tasks match the current filters.";
    row.appendChild(cell);
    els.tasksBody.appendChild(row);
    return;
  }

  tasks
    .slice()
    .sort((a, b) => String(a.status).localeCompare(String(b.status)) || String(a.id).localeCompare(String(b.id)))
    .forEach((task) => {
      const row = document.createElement("tr");
      row.className = "task-row";
      if (isSelected("task", task.id)) {
        row.style.background = "rgba(15, 108, 91, 0.08)";
      }
      row.innerHTML = `
        <td class="mono">${task.id}</td>
        <td>${task.namespace || "default"}</td>
        <td>${task.status}</td>
        <td>${task.node_id || "-"}</td>
        <td>${task.image}</td>
        <td>${task.attempts}/${task.max_attempts}</td>
        <td>${task.secret_refs?.length || 0}</td>
        <td>${task.volumes?.length || 0}</td>
      `;
      row.title = task.status_detail || task.last_error || "";
      row.addEventListener("click", () => setSelected("task", task));
      els.tasksBody.appendChild(row);
    });
}

function detailRows(rows) {
  const grid = document.createElement("div");
  grid.className = "detail-grid";
  rows.forEach(([label, value]) => {
    const row = document.createElement("div");
    row.className = "detail-row";
    const left = document.createElement("span");
    left.textContent = label;
    const right = document.createElement("span");
    right.textContent = value;
    row.append(left, right);
    grid.appendChild(row);
  });
  return grid;
}

function renderDetailPanel(tasks) {
  els.detailPanel.replaceChildren();
  if (!state.selected) {
    els.detailPanel.appendChild(emptyState("Click a node, job, service, or task to inspect it."));
    return;
  }

  const block = document.createElement("div");
  block.className = "detail-block";
  const title = document.createElement("div");
  title.className = "detail-title";

  switch (state.selected.type) {
    case "node": {
      const node = state.selected.payload;
      const nodeTasks = tasks.filter((task) => task.node_id === node.id);
      title.textContent = `Node ${node.id}`;
      block.append(
        title,
        detailRows([
          ["Status", node.status],
          ["CPU", `${node.used_cpu}/${node.total_cpu}`],
          ["Memory", `${node.used_memory}/${node.total_memory} MB`],
          ["Reliability", Number(node.reliability || 0).toFixed(2)],
          ["Tasks on node", String(nodeTasks.length)],
        ]),
      );
      break;
    }
    case "job": {
      const job = state.selected.payload;
      const jobTasks = tasks.filter((task) => task.job_id === job.id);
      title.textContent = `Job ${job.id}`;
      block.append(
        title,
        detailRows([
          ["Namespace", job.namespace || "default"],
          ["Status", job.status],
          ["Image", job.image],
          ["Replicas", String(job.replicas)],
          ["Tracked tasks", String(jobTasks.length)],
          ["Secret refs", String(job.secret_refs?.length || 0)],
          ["Volumes", String(job.volumes?.length || 0)],
        ]),
      );
      break;
    }
    case "service": {
      const service = state.selected.payload;
      const key = `${service.namespace}/${service.name}`;
      const status = state.serviceStatus[key];
      const summary = status?.summary || service.summary || {};
      title.textContent = `Service ${service.namespace}/${service.name}`;
      block.append(
        title,
        detailRows([
          ["Job", service.selector_job_id],
          ["Port", String(service.port)],
          ["Namespace", service.namespace],
          ["Healthy endpoints", String(summary.healthy_endpoints || 0)],
          ["Degraded endpoints", String(summary.degraded_endpoints || 0)],
          ["Unhealthy endpoints", String(summary.unhealthy_endpoints || 0)],
          ["Eligible endpoints", String(summary.eligible_endpoints || 0)],
        ]),
      );
      if (status?.endpoints?.length) {
        const endpointList = document.createElement("div");
        endpointList.className = "detail-grid";
        status.endpoints.forEach((endpoint) => {
          const row = document.createElement("div");
          row.className = "detail-row";
          const left = document.createElement("span");
          left.textContent = `${endpoint.task_id} @ ${endpoint.node_id}`;
          const right = document.createElement("span");
          right.textContent = `${endpoint.health} • ${endpoint.reason}`;
          row.append(left, right);
          endpointList.appendChild(row);
        });
        block.append(endpointList);
      }
      break;
    }
    case "namespace": {
      const ns = state.selected.payload;
      title.textContent = `Namespace ${ns.name}`;
      block.append(
        title,
        detailRows([
          ["CPU quota", String(ns.cpu_quota || "unbounded")],
          ["Memory quota", ns.memory_quota ? `${ns.memory_quota} MB` : "unbounded"],
          ["Task quota", String(ns.task_quota || "unbounded")],
        ]),
      );
      break;
    }
    case "task": {
      const task = state.selected.payload;
      title.textContent = `Task ${task.id}`;
      block.append(
        title,
        detailRows([
          ["Namespace", task.namespace || "default"],
          ["Status", task.status],
          ["Image", task.image],
          ["Node", task.node_id || "-"],
          ["Attempts", `${task.attempts}/${task.max_attempts}`],
          ["Detail", task.status_detail || task.last_error || "No additional detail available"],
          ["Secret refs", String(task.secret_refs?.length || 0)],
          ["Volumes", String(task.volumes?.length || 0)],
        ]),
      );
      break;
    }
    default:
      block.append(title, emptyState());
  }

  els.detailPanel.appendChild(block);
}

function populateNamespaceFilter(namespaces) {
  const existing = new Set(Array.from(els.namespaceFilter.options).map((option) => option.value));
  namespaces.forEach((ns) => {
    if (existing.has(ns.name)) {
      return;
    }
    const option = document.createElement("option");
    option.value = ns.name;
    option.textContent = ns.name;
    els.namespaceFilter.appendChild(option);
  });
}

function setStatus(message, isError = false) {
  els.connectionStatus.textContent = message;
  els.connectionStatus.style.color = isError ? "var(--danger)" : "var(--muted)";
}

function renderAll() {
  const tasks = filterTasks(state.cluster.tasks || []);
  const jobs = filterJobs(state.jobs || []);
  const services = filterServices(state.services || []);
  const nodes = filterNodes(state.cluster.nodes || []);
  const selectedExists = (() => {
    if (!state.selected) {
      return true;
    }
    switch (state.selected.type) {
      case "task":
        return tasks.some((task) => task.id === state.selected.id);
      case "job":
        return jobs.some((job) => job.id === state.selected.id);
      case "node":
        return nodes.some((node) => node.id === state.selected.id);
      case "service":
        return services.some((service) => `${service.namespace}/${service.name}` === state.selected.id);
      case "namespace":
        return state.namespaces.some((ns) => ns.name === state.selected.id);
      default:
        return true;
    }
  })();
  if (!selectedExists) {
    state.selected = null;
  }

  renderStats(state.cluster.nodes || [], state.cluster.tasks || [], state.jobs || [], state.services || []);
  renderNodes(nodes, state.cluster.tasks || []);
  renderJobs(jobs);
  renderServices(services);
  renderNamespaces(state.namespaces || []);
  renderInsights(state.insights || []);
  renderTasks(tasks);
  renderDetailPanel(state.cluster.tasks || []);
}

async function refreshDashboard() {
  const settings = getSettings();
  saveSettings(settings);
  state.loading = true;
  setStatus("Refreshing cluster data...");

  try {
    const [cluster, jobsPayload, namespacesPayload, servicesPayload, insightsPayload] = await Promise.all([
      fetchJSON("/cluster"),
      fetchJSON("/jobs"),
      fetchJSON("/namespaces"),
      fetchJSON("/services"),
      fetchJSON("/insights"),
    ]);

    state.cluster = cluster;
    state.jobs = jobsPayload.jobs || [];
    state.namespaces = namespacesPayload.namespaces || [];
    state.services = servicesPayload.services || [];
    state.insights = insightsPayload.insights || [];
    populateNamespaceFilter(state.namespaces);
    renderAll();
    els.refreshNote.textContent = `Last refresh: ${new Date().toLocaleTimeString()}`;
    setStatus(`Connected to ${settings.baseURL}`);
  } catch (error) {
    console.error(error);
    setStatus(`Request failed: ${error.message}`, true);
  } finally {
    state.loading = false;
  }
}

function bindEvents() {
  els.connectButton.addEventListener("click", refreshDashboard);
  els.refreshButton.addEventListener("click", refreshDashboard);
  els.namespaceFilter.addEventListener("change", (event) => {
    state.filters.namespace = event.target.value;
    renderAll();
  });
  els.statusFilter.addEventListener("change", (event) => {
    state.filters.status = event.target.value;
    renderAll();
  });
  els.searchFilter.addEventListener("input", (event) => {
    state.filters.search = event.target.value;
    renderAll();
  });
  els.clearFiltersButton.addEventListener("click", () => {
    state.filters = { namespace: "", status: "", search: "" };
    els.namespaceFilter.value = "";
    els.statusFilter.value = "";
    els.searchFilter.value = "";
    renderAll();
  });
}

function init() {
  const settings = loadSettings();
  els.baseURL.value = settings.baseURL || "http://localhost:8080";
  bindEvents();
  refreshDashboard();
}

init();
