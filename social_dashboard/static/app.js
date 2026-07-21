/* Kan News social dashboard — shared client runtime (vanilla, no build) */
(function () {
  "use strict";

  // ---------- icons (inner SVG markup) ----------
  var FILL = {
    yt: '<path d="M23.5 6.2a3 3 0 0 0-2.1-2.1C19.5 3.6 12 3.6 12 3.6s-7.5 0-9.4.5A3 3 0 0 0 .5 6.2C0 8.1 0 12 0 12s0 3.9.5 5.8a3 3 0 0 0 2.1 2.1c1.9.5 9.4.5 9.4.5s7.5 0 9.4-.5a3 3 0 0 0 2.1-2.1c.5-1.9.5-5.8.5-5.8s0-3.9-.5-5.8zM9.6 15.6V8.4l6.2 3.6-6.2 3.6z"/>',
    fb: '<path d="M24 12.07C24 5.4 18.6 0 12 0S0 5.4 0 12.07c0 6 4.4 10.95 10.13 11.85v-8.38H7.08v-3.47h3.05V9.43c0-3 1.79-4.67 4.53-4.67 1.31 0 2.69.24 2.69.24v2.95h-1.51c-1.49 0-1.96.93-1.96 1.87v2.25h3.33l-.53 3.47h-2.8v8.38C19.6 23.02 24 18.06 24 12.07z"/>',
    ig: '<path d="M12 2.16c3.2 0 3.58.01 4.85.07 3.25.15 4.77 1.69 4.92 4.92.06 1.27.07 1.65.07 4.85s-.01 3.58-.07 4.85c-.15 3.23-1.66 4.77-4.92 4.92-1.27.06-1.64.07-4.85.07s-3.58-.01-4.85-.07c-3.26-.15-4.77-1.7-4.92-4.92-.06-1.27-.07-1.64-.07-4.85s.01-3.58.07-4.85C2.38 3.93 3.9 2.38 7.15 2.23 8.42 2.17 8.8 2.16 12 2.16zM12 0C8.74 0 8.33.01 7.05.07 2.7.27.28 2.69.08 7.05.01 8.33 0 8.74 0 12s.01 3.67.07 4.95c.2 4.36 2.62 6.78 6.98 6.98C8.33 23.99 8.74 24 12 24s3.67-.01 4.95-.07c4.35-.2 6.78-2.62 6.98-6.98.06-1.28.07-1.69.07-4.95s-.01-3.67-.07-4.95c-.2-4.35-2.62-6.78-6.98-6.98C15.67.01 15.26 0 12 0zm0 5.84a6.16 6.16 0 1 0 0 12.32 6.16 6.16 0 0 0 0-12.32zM12 16a4 4 0 1 1 0-8 4 4 0 0 1 0 8zm6.41-10.85a1.44 1.44 0 1 0 0 2.88 1.44 1.44 0 0 0 0-2.88z"/>',
    x: '<path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>',
    tiktok: '<path d="M12.53.02C13.84 0 15.14.01 16.44 0c.08 1.53.63 3.09 1.75 4.17 1.12 1.11 2.7 1.62 4.24 1.79v4.03c-1.44-.05-2.89-.35-4.2-.97-.57-.26-1.1-.59-1.62-.93-.01 2.92.01 5.84-.02 8.75-.08 1.4-.54 2.79-1.35 3.94-1.31 1.92-3.58 3.17-5.91 3.21-1.43.08-2.86-.31-4.08-1.03-2.02-1.19-3.44-3.37-3.65-5.71-.02-.5-.03-1-.01-1.49.18-1.9 1.12-3.72 2.58-4.96 1.66-1.44 3.98-2.13 6.15-1.72.02 1.48-.04 2.96-.04 4.44-.99-.32-2.15-.23-3.02.37-.63.41-1.11 1.04-1.36 1.75-.21.51-.15 1.07-.14 1.61.24 1.64 1.82 3.02 3.5 2.87 1.12-.01 2.19-.66 2.77-1.61.19-.33.4-.67.41-1.06.1-1.79.06-3.57.07-5.36.01-4.03-.01-8.05.02-12.07z"/>',
    users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM22 21v-2a4 4 0 0 0-3-3.87M16 3.13A4 4 0 0 1 16 11"/>'
  };
  var STROKE = {
    eye: '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/>',
    reach: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="4"/>',
    like: '<path d="M7 10v11M2 10v11M7 10l4-7a2 2 0 0 1 3 1v4h5a2 2 0 0 1 2 2.3l-1.3 7A2 2 0 0 1 17 21H7"/>',
    comment: '<path d="M21 11.5a8.4 8.4 0 0 1-9 8.5 9 9 0 0 1-4-1L3 21l1.9-5A8.4 8.4 0 0 1 12 3a8.4 8.4 0 0 1 9 8.5z"/>',
    share: '<circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><path d="M8.6 13.5l6.8 4M15.4 6.5l-6.8 4"/>',
    save: '<path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/>',
    heart: '<path d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.7l-1-1.1a5.5 5.5 0 1 0-7.8 7.8L12 21l8.8-8.6a5.5 5.5 0 0 0 0-7.8z"/>',
    video: '<rect x="2" y="4" width="20" height="16" rx="2"/><path d="M10 9l5 3-5 3z"/>',
    users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z"/>',
    sparkles: '<path d="M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9zM19 16l.7 2 2 .7-2 .7-.7 2-.7-2-2-.7 2-.7z"/>'
  };
  var SUN = '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M19.1 4.9l-1.4 1.4M6.3 17.7l-1.4 1.4"/>';
  var MOON = '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>';
  var KAN = '<svg viewBox="0 0 571.48 572.69" width="28" height="28" style="flex:none;"><path fill="#ff3300" d="M.89,77.51v418.03c0,42.22,34.22,76.44,76.44,76.44h418.03c42.22,0,76.44-34.22,76.44-76.44V77.51c0-42.22-34.22-76.44-76.44-76.44H77.33C35.11,1.07.89,35.3.89,77.51ZM282.6,373.8l-34.17,34.17-87.28-87.27-34.17-34.17,34.17-34.17,87.28-87.27,34.17,34.17-87.28,87.27,87.28,87.27ZM377.36,450.54h-47.38V122.51h47.38v328.03Z"></path></svg>';

  function fillIcon(name, color, size) {
    size = size || 19;
    return '<svg width="' + size + '" height="' + size + '" viewBox="0 0 24 24" style="fill:' + (color || 'currentColor') + ';">' + FILL[name] + '</svg>';
  }
  function strokeIcon(name, size) {
    size = size || 15;
    return '<svg width="' + size + '" height="' + size + '" viewBox="0 0 24 24" fill="none" style="stroke:currentColor;" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' + STROKE[name] + '</svg>';
  }

  // ---------- formatting ----------
  function fmt(n) {
    n = Math.round(Number(n) || 0);
    if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1).replace(/\.0$/, "") + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1).replace(/\.0$/, "") + "K";
    return "" + n;
  }
  function fmtHtml(n) {
    // big-number value with the K/M shown as a small, lighter unit (not glued black)
    var s = fmt(n), m = s.match(/^([\d.]+)([KM])$/);
    return m ? m[1] + '<span class="unit">' + m[2] + "</span>" : s;
  }
  function fmtDate(iso) {
    if (!iso) return "";
    var p = iso.split("-");
    if (p.length < 3) return iso;
    return parseInt(p[2], 10) + "/" + parseInt(p[1], 10);
  }
  function fmtFullDate(iso) {
    if (!iso) return "";
    var months = ["ינואר", "פברואר", "מרץ", "אפריל", "מאי", "יוני", "יולי", "אוגוסט", "ספטמבר", "אוקטובר", "נובמבר", "דצמבר"];
    var p = iso.split("-");
    if (p.length < 3) return iso;
    return parseInt(p[2], 10) + " ב" + months[parseInt(p[1], 10) - 1] + " " + p[0];
  }
  function signed(n) { return (n >= 0 ? "+" : "") + fmt(n); }
  function delta(cur, prev) {
    var v = prev > 0 ? (cur - prev) / prev * 100 : 0;
    var up = v >= 0;
    return { txt: Math.abs(v).toFixed(1) + "%", cls: up ? "up" : "down", arrow: up ? "▲" : "▼" };
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }
  // minimal, safe inline markdown for the AI insight text: escape first (so the
  // sheet/Gemini content can't inject HTML), then render **bold** only.
  function mdInline(s) {
    return esc(s).replace(/\*\*([^*]+?)\*\*/g, "<strong>$1</strong>");
  }

  // ---------- charts (return SVG strings; colors may be CSS vars) ----------
  function donutSvg(segments, centerNum, centerLabel) {
    var r = 56, C = 2 * Math.PI * r;
    var total = segments.reduce(function (a, s) { return a + s.value; }, 0) || 1;
    var off = 0;
    var arcs = segments.map(function (s) {
      var len = s.value / total * C;
      var o = '<circle cx="75" cy="75" r="' + r + '" fill="none" stroke="' + s.color + '" stroke-width="20" stroke-dasharray="' + len.toFixed(1) + " " + (C - len).toFixed(1) + '" stroke-dashoffset="' + (-off).toFixed(1) + '"></circle>';
      off += len; return o;
    }).join("");
    return '<svg viewBox="0 0 150 150" width="158" height="158"><g transform="rotate(-90 75 75)">'
      + '<circle cx="75" cy="75" r="' + r + '" fill="none" stroke="var(--chip)" stroke-width="20"></circle>' + arcs + "</g>"
      + '<text x="75" y="70" text-anchor="middle" style="fill:var(--text);font-weight:700;font-size:26px;font-family:\'SimplerPro\',sans-serif;">' + centerNum + "</text>"
      + '<text x="75" y="89" text-anchor="middle" style="fill:var(--muted);font-size:11px;font-family:ui-monospace,monospace;">' + centerLabel + "</text></svg>";
  }

  function sparkSvg(data, color) {
    var a = data.slice(-14);
    if (!a.length) a = [0, 0];
    var mx = Math.max.apply(null, a) * 1.1 || 1, mn = Math.min.apply(null, a) * 0.92;
    var span = (mx - mn) || 1;
    var sx = function (i) { return a.length <= 1 ? 0 : i * 240 / (a.length - 1); };
    var sy = function (v) { return 48 - ((v - mn) / span) * 44 - 2; };
    var pts = a.map(function (v, i) { return sx(i).toFixed(1) + "," + sy(v).toFixed(1); });
    return '<svg viewBox="0 0 240 50" width="100%" height="40"><path d="M 0,50 L ' + pts.join(" L ") + ' L 240,50 Z" fill="' + color + '" fill-opacity="0.1"></path>'
      + '<polyline points="' + pts.join(" ") + '" fill="none" stroke="' + color + '" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"></polyline></svg>';
  }

  // ---------- interactive line chart (hover guide + dots + tooltip) ----------
  var _charts = {}, _cn = 0, _tip = null;

  function _tooltip() {
    if (_tip) return _tip;
    _tip = document.createElement("div");
    _tip.style.cssText = "position:fixed;z-index:50;pointer-events:none;display:none;min-width:120px;white-space:nowrap;" +
      "background:var(--panel);border:1px solid var(--line2);border-radius:7px;padding:7px 10px;font-size:12px;" +
      "color:var(--text);box-shadow:0 4px 16px rgba(0,0,0,.25);font-family:'SimplerPro','Segoe UI',sans-serif;";
    document.body.appendChild(_tip);
    return _tip;
  }

  function chart(opts) {
    var id = "c" + (++_cn);
    var dates = opts.dates, series = opts.series, vbW = opts.vbW || 720, vbH = opts.vbH || 240;
    var padT = opts.padT || 12, padB = opts.padB || 28, n = dates.length;
    var all = []; series.forEach(function (s) { all = all.concat(s.data); });
    var max = Math.max.apply(null, all.concat([1])) * 1.12;
    var iH = vbH - padT - padB;
    var X = function (i) { return n <= 1 ? 0 : i * vbW / (n - 1); };
    var Y = function (v) { return padT + (1 - v / max) * iH; };
    var grid = [0, .25, .5, .75, 1].map(function (f) {
      var y = padT + (1 - f) * iH;
      return '<line class="grid-line" x1="0" x2="' + vbW + '" y1="' + y.toFixed(1) + '" y2="' + y.toFixed(1) + '"></line>'
        + '<text class="grid-text" x="' + (vbW - 2) + '" y="' + (y - 3).toFixed(1) + '" text-anchor="end">' + fmt(max * f) + '</text>';
    }).join("");
    var areas = "", lines = "", dots = "";
    series.forEach(function (s, si) {
      var pts = s.data.map(function (v, i) { return X(i).toFixed(1) + "," + Y(v).toFixed(1); }).join(" ");
      if (s.area) {
        var baseY = (padT + iH).toFixed(1);
        areas += '<path d="M ' + X(0).toFixed(1) + "," + baseY + " L " + s.data.map(function (v, i) { return X(i).toFixed(1) + "," + Y(v).toFixed(1); }).join(" L ") + " L " + X(n - 1).toFixed(1) + "," + baseY + ' Z" fill="' + s.color + '" fill-opacity="0.13"></path>';
      }
      lines += '<polyline points="' + pts + '" fill="none" stroke="' + s.color + '" stroke-width="' + (s.w || 2.4) + '" stroke-linejoin="round" stroke-linecap="round"></polyline>';
      dots += '<circle class="ch-dot" data-si="' + si + '" r="3.6" fill="' + s.color + '" stroke="var(--panel)" stroke-width="1.5" style="display:none"></circle>';
    });
    var lc = Math.min(6, n), xl = "";
    for (var k = 0; k < lc; k++) {
      var i = Math.round(k * (n - 1) / (lc - 1 || 1));
      xl += '<text class="grid-text" x="' + X(i).toFixed(1) + '" y="' + (vbH - 4) + '" text-anchor="middle">' + fmtDate(dates[i]) + "</text>";
    }
    var guide = '<line class="ch-guide" y1="' + padT + '" y2="' + (padT + iH).toFixed(1) + '" style="display:none;stroke:var(--line2);stroke-width:1;stroke-dasharray:3 3;"></line>';
    _charts[id] = { dates: dates, series: series, vbW: vbW, padT: padT, iH: iH, max: max, n: n };
    return '<svg data-chart="' + id + '" viewBox="0 0 ' + vbW + " " + vbH + '" width="100%" style="display:block;overflow:visible;cursor:crosshair;">' + grid + areas + lines + guide + dots + xl + '</svg>';
  }

  function wireCharts() {
    document.querySelectorAll("svg[data-chart]").forEach(function (svg) {
      if (svg._wired) return; svg._wired = true;
      var cfg = _charts[svg.getAttribute("data-chart")]; if (!cfg) return;
      var guide = svg.querySelector(".ch-guide"), dots = svg.querySelectorAll(".ch-dot"), tip = _tooltip();
      var X = function (i) { return cfg.n <= 1 ? 0 : i * cfg.vbW / (cfg.n - 1); };
      var Y = function (v) { return cfg.padT + (1 - v / cfg.max) * cfg.iH; };
      function move(e) {
        var r = svg.getBoundingClientRect();
        if (!r.width) return;
        var i = Math.round((e.clientX - r.left) / r.width * cfg.vbW / (cfg.vbW / ((cfg.n - 1) || 1)));
        if (i < 0) i = 0; if (i > cfg.n - 1) i = cfg.n - 1;
        var gx = X(i).toFixed(1);
        guide.setAttribute("x1", gx); guide.setAttribute("x2", gx); guide.style.display = "";
        var rows = "";
        cfg.series.forEach(function (s, si) {
          var v = s.data[i], d = dots[si];
          d.setAttribute("cx", gx); d.setAttribute("cy", Y(v).toFixed(1)); d.style.display = "";
          rows += '<div style="display:flex;align-items:center;gap:6px;margin-top:3px;">' +
            '<span style="width:9px;height:9px;border-radius:2px;background:' + s.color + ';flex:none;"></span>' +
            (s.name ? '<span style="color:var(--muted);">' + s.name + "</span>" : "") +
            '<b style="margin-inline-start:auto;font-family:ui-monospace,monospace;">' + fmt(v) + "</b></div>";
        });
        tip.innerHTML = '<div style="font-family:ui-monospace,monospace;font-size:11px;color:var(--faint);">' + fmtDate(cfg.dates[i]) + "</div>" + rows;
        tip.style.display = "";
        var tx = e.clientX + 14, ty = e.clientY + 14;
        if (tx + 170 > window.innerWidth) tx = e.clientX - 170;
        tip.style.left = tx + "px"; tip.style.top = ty + "px";
      }
      function leave() { guide.style.display = "none"; dots.forEach(function (d) { d.style.display = "none"; }); tip.style.display = "none"; }
      svg.addEventListener("pointermove", move);
      svg.addEventListener("pointerleave", leave);
    });
  }

  // ---------- drill-down modal (shared chrome; pages supply the body) ----------
  var _modalEl = null;
  function closeModal() {
    if (_modalEl) { _modalEl.remove(); _modalEl = null; document.removeEventListener("keydown", _modalKey); }
  }
  function _modalKey(e) { if (e.key === "Escape") closeModal(); }
  function openModal(opts) {
    closeModal();
    var head =
      '<div class="kmodal-head">' +
        (opts.iconHtml ? '<span class="km-plat" style="background:' + (opts.iconBg || "var(--chip)") + ';">' + opts.iconHtml + "</span>" : "") +
        '<div class="km-head-txt">' +
          (opts.type ? '<div class="km-type">' + esc(opts.type) + "</div>" : "") +
          '<div class="km-title">' + esc(opts.title || "") + "</div>" +
          (opts.date ? '<div class="km-date">' + esc(opts.date) + "</div>" : "") +
        "</div>" +
        '<button class="kmodal-close" title="סגור" aria-label="סגור">✕</button>' +
      "</div>";
    var el = document.createElement("div");
    el.className = "kmodal-backdrop";
    el.innerHTML = '<div class="kmodal">' + head + '<div class="kmodal-body">' + (opts.bodyHtml || "") + "</div></div>";
    el.addEventListener("click", function (e) { if (e.target === el) closeModal(); });
    el.querySelector(".kmodal-close").addEventListener("click", closeModal);
    document.body.appendChild(el);
    document.addEventListener("keydown", _modalKey);
    _modalEl = el;
    return el;
  }
  // small builders so the platform pages stay tidy
  function kmCell(label, valueHtml, unit) {
    return '<div class="km-cell"><div class="l">' + esc(label) + '</div><div class="v">' + valueHtml +
      (unit ? ' <span class="unit">' + esc(unit) + "</span>" : "") + "</div></div>";
  }
  function kmSection(label) { return '<div class="km-seclabel">' + esc(label) + "</div>"; }
  function kmCmp(label, ratio) {
    // ratio >= 1 shown as "×N", colored by whether higher is good (up) — caller
    // passes the sign via a leading "-" on label is avoided; we just color up/down by >=1
    var up = ratio >= 1;
    return '<span class="km-chip"><span class="l">' + esc(label) + '</span><span class="r ' + (up ? "up" : "down") +
      '">×' + ratio.toFixed(1) + "</span></span>";
  }
  // Gemini comment-analysis block for the drill-down modal (IG + FB pages)
  function kmAnalysis(an) {
    if (!an) return "";
    var senti = '<div class="km-senti"><div class="bar">' +
      '<span class="pos" style="width:' + an.pos + '%"></span>' +
      '<span class="neu" style="width:' + an.neu + '%"></span>' +
      '<span class="neg" style="width:' + an.neg + '%"></span></div>' +
      '<div class="legend"><span class="p">חיובי ' + an.pos + '%</span><span>ניטרלי ' + an.neu + '%</span><span class="n">שלילי ' + an.neg + '%</span></div>' +
      (an.critique ? '<div class="km-critique">📣 ביקורת על הסיקור עצמו: ' + an.critique + '% מהתגובות</div>' : '') + '</div>';
    var themes = an.themes.length ? '<div class="km-themes">' + an.themes.map(function (t) { return '<span class="km-theme">' + esc(t) + '</span>'; }).join("") + '</div>' : "";
    var quotes = an.top_comments.length ? '<div class="km-quotes">' + an.top_comments.map(function (q) { return '<div class="km-quote">' + esc(q) + '</div>'; }).join("") + '</div>' : "";
    return kmSection("ניתוח שיחה · AI" + (an.controversy ? " · 🔥 שיחה טעונה" : "")) +
      senti + (an.why ? '<p class="km-why">' + esc(an.why) + '</p>' : "") + themes + quotes +
      '<div class="km-ai-note">מבוסס על ' + an.n + ' תגובות · ניתוח Gemini</div>';
  }
  // "יש ניתוח שיחה" label for the table date line
  function aiDot(p) { return p.analysis ? '<span class="ai-dot" title="יש ניתוח שיחה">💬 ניתוח שיחה</span>' : ""; }

  // ---------- theme + state ----------
  function getTheme() { try { return localStorage.getItem("pm_theme") || "dark"; } catch (e) { return "dark"; } }
  function setTheme(t) { try { localStorage.setItem("pm_theme", t); } catch (e) {} document.documentElement.setAttribute("data-theme", t); }
  function getRange() { try { return parseInt(localStorage.getItem("pm_range") || "7", 10); } catch (e) { return 7; } }
  function setRange(r) { try { localStorage.setItem("pm_range", r); } catch (e) {} }

  var RANGE_LABEL = { 7: "7 הימים האחרונים", 14: "14 הימים האחרונים", 30: "30 הימים האחרונים", 90: "90 הימים האחרונים" };

  var NAV = [
    { key: "overview", href: "/", label: "סקירה כללית", icon: null },
    { key: "youtube", href: "/youtube", label: "YouTube", icon: fillIcon("yt", "#ff0000", 15) },
    { key: "facebook", href: "/facebook", label: "Facebook", icon: fillIcon("fb", "#1877f2", 15) },
    { key: "instagram", href: "/instagram", label: "Instagram", icon: fillIcon("ig", "#e4405f", 15) },
    { key: "twitter", href: "/twitter", label: "X", icon: fillIcon("x", "var(--text)", 13) },
    { key: "tiktok", href: "/tiktok", label: "TikTok", icon: fillIcon("tiktok", "var(--text)", 13) },
    { key: "competitors", href: "/competitors", label: "מתחרים", icon: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" style="stroke:currentColor;" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>' },
    { key: "viral", href: "/viral", label: "ויראלי", icon:'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" style="stroke:currentColor;" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg>' },
    { key: "alerts", href: "/alerts", label: "התראות", icon:'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" style="stroke:currentColor;" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9M13.7 21a2 2 0 0 1-3.4 0"/></svg>' }
  ];

  var app = {
    page: "overview",
    range: 7,
    theme: "dark",
    pushstat: "https://pushstat.benzcohq.com/",
    onRender: null,

    buildHeader: function () {
      var self = this;
      var nav = NAV.map(function (n) {
        return '<a href="' + n.href + '" class="' + (n.key === self.page ? "active" : "") + '">' + (n.icon || "") + n.label + "</a>";
      }).join("");
      var ranges = [7, 14, 30, 90].map(function (r) {
        return '<button class="range ' + (r === self.range ? "active" : "") + '" data-range="' + r + '">' + r + "</button>";
      }).join("");
      var themeIcon = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' + (self.theme === "dark" ? SUN : MOON) + "</svg>";
      var refreshIcon = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 2v6h-6M3 12a9 9 0 0 1 15-6.7L21 8M3 22v-6h6M21 12a9 9 0 0 1-15 6.7L3 16"/></svg>';
      var extIcon = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" style="stroke:currentColor;" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6M15 3h6v6M10 14L21 3"/></svg>';
      var html =
        '<div class="header-stripe"></div>' +
        '<div class="header-inner"><div class="header-row">' +
          '<div class="brand">' + KAN +
            '<div><div class="brand-title">כאן חדשות <span>· סושיאל</span></div>' +
            '<div class="brand-sub">SOCIAL ANALYTICS</div></div></div>' +
          '<nav class="nav">' + nav + "</nav>" +
          '<a class="ext-link" href="' + self.pushstat + '">' + extIcon + "פושים</a>" +
          '<span class="spacer"></span>' +
          '<div class="ranges">' + ranges + "</div>" +
          '<button class="iconbtn" id="btnRefresh" title="רענון">' + refreshIcon + "</button>" +
          '<button class="iconbtn" id="btnTheme" title="החלפת מצב תצוגה">' + themeIcon + "</button>" +
        "</div></div>";
      var el = document.getElementById("header");
      el.className = "header";
      el.innerHTML = html;
      el.querySelectorAll(".range").forEach(function (b) {
        b.addEventListener("click", function () {
          self.range = parseInt(b.getAttribute("data-range"), 10);
          setRange(self.range);
          self.buildHeader();
          self.load();
        });
      });
      document.getElementById("btnTheme").addEventListener("click", function () {
        self.theme = self.theme === "dark" ? "light" : "dark";
        setTheme(self.theme);
        self.buildHeader();
        if (self._data) self.render(self._data); // redraw for theme-tied colors
      });
      document.getElementById("btnRefresh").addEventListener("click", function () { self.load(true); });
    },

    render: function (data) {
      this._data = data;
      if (this.onRender) this.onRender(data, app);
    },

    load: function (force) {
      var self = this;
      var url = "/api/" + this.page + "?days=" + this.range + (force ? "&refresh=1" : "");
      var main = document.getElementById("main");
      fetch(url).then(function (r) { return r.json(); }).then(function (data) {
        if (data && data.error) { main.innerHTML = '<div class="panel" style="margin-top:20px;color:var(--bad);">שגיאה בטעינת נתונים: ' + esc(data.error) + "</div>"; return; }
        if (data && data.pushstat_url) { self.pushstat = data.pushstat_url; self.buildHeader(); }
        self.render(data);
      }).catch(function (e) {
        main.innerHTML = '<div class="panel" style="margin-top:20px;color:var(--bad);">שגיאת רשת: ' + esc(e.message) + "</div>";
      });
    },

    init: function (page, onRender) {
      this.page = page;
      this.theme = getTheme();
      this.range = getRange();
      this.onRender = onRender;
      document.body.setAttribute("data-page", page);
      setTheme(this.theme);
      this.buildHeader();
      this.load();
    }
  };

  // expose helpers
  window.KanSocial = app;
  window.KS = {
    fmt: fmt, fmtHtml: fmtHtml, fmtDate: fmtDate, fmtFullDate: fmtFullDate, signed: signed, delta: delta, esc: esc, mdInline: mdInline,
    fillIcon: fillIcon, strokeIcon: strokeIcon, chart: chart, wireCharts: wireCharts, donutSvg: donutSvg, sparkSvg: sparkSvg,
    openModal: openModal, closeModal: closeModal, kmCell: kmCell, kmSection: kmSection, kmCmp: kmCmp,
    kmAnalysis: kmAnalysis, aiDot: aiDot,
    RANGE_LABEL: RANGE_LABEL, FILL: FILL
  };
})();
