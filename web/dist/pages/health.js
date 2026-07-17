export const healthMethods = {
  startHealthPoll() {
    // 1s tick keeps uptime / age / expires-in countdowns live between 12s fetches.
    if (!this._healthTick) this._healthTick = setInterval(() => { if (this.state.view === 'health') this._updateHealthLiveCells(); }, 1000);
    if (this._healthPoll) return;
    this._healthPoll = setInterval(() => { if (this.state.view !== 'health') return; this.fetchHealth(true); }, 12000);
  },
  stopHealthPoll() {
    try { clearInterval(this._healthPoll); } catch (e) {} this._healthPoll = null;
    try { clearInterval(this._healthTick); } catch (e) {} this._healthTick = null;
  },

  // Health page. Body-appended, position:fixed overlay (same pattern).
  // /api/health, polled every 12s; a 1s tick keeps uptime / lock age /
  // expires-in countdowns live between fetches. Row clicks open the same
  // job-detail panel the Jobs page uses (via the shared _jobDetailHTML).
  // ============================================================
  _fmtCountdown(ms) {
    if (ms == null) return '—';
    if (ms <= 0) return 'expired';
    let s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600); s -= h * 3600;
    const m = Math.floor(s / 60); s -= m * 60;
    if (h > 0) return h + 'h ' + m + 'm';
    if (m > 0) return m + 'm ' + s + 's';
    return s + 's';
  },

  ensureHealthDom() {
    if (this._healthRoot) return this._healthRoot;
    this._ensureDetailStyles();
    const root = document.createElement('div');
    root.id = 'health-root';
    root.style.cssText = 'position:fixed;left:216px;top:54px;right:0;bottom:0;z-index:40;background:#07080f;display:none;font-family:Inter,system-ui,sans-serif;color:#c8cce6';
    root.innerHTML = [
      '<div class="hl-body">',
      '<div id="hl-banner"></div>',
      '<div class="hl-vitals" id="hl-vitals"></div>',
      '<div class="hl-second" id="hl-second"></div>',
      '<div id="hl-stuck-sec"><div class="hl-h">STUCK JOBS <span class="hl-hc" id="hl-stuck-c"></span></div><div id="hl-stuck"></div></div>',
      '<div id="hl-locks-sec"><div class="hl-h">LOCKS <span class="hl-hsub">amber &gt;24h &middot; red &gt;7d</span></div><div id="hl-locks"></div></div>',
      '</div>'
    ].join('\n');
    document.body.appendChild(root);
    this._dockOverlay(root); // mobile: left:0 + bottom clears the tab bar
    this._healthRoot = root;

    // Delegated click handling for the whole overlay: copy chips, the
    // preview-all-clear toggle, per-row "why" expanders, agent cross-links, and
    // stuck-row -> in-place job detail (the last, so a bare row click still opens).
    root.addEventListener('click', (e) => {
      const el = (e.target && e.target.closest) ? e.target : (e.target && e.target.parentElement);
      const q = (sel) => (el && el.closest) ? el.closest(sel) : null;
      const copy = q('.gm-copy');
      if (copy) { e.stopPropagation(); this._gmCopyChip(copy); return; }
      const tog = q('[data-hl-toggle]');
      if (tog) { e.stopPropagation(); this._hlOverride = this._hlAllClear ? 'live' : 'clear'; this.renderHealth(this._healthData); return; }
      const why = q('[data-hl-why]');
      if (why) { e.stopPropagation(); this._hlToggleWhy(why); return; }
      const ag = q('[data-agent]');
      if (ag) { e.stopPropagation(); this._hlGotoAgent(ag.getAttribute('data-agent')); return; }
      const row = q('.hl-srow[data-id]');
      if (row) this.openHealthDetail(row.getAttribute('data-id'));
    });
    this._healthKeydown = (e) => { if (e.key === 'Escape' && this.state.view === 'health' && this._healthSelected) this.closeHealthDetail(); };
    document.addEventListener('keydown', this._healthKeydown);
    return root;
  },

  fetchHealth(isPoll) {
    const seq = ++this._healthFetchSeq;
    fetch('/api/health', { cache: 'no-store' })
      .then(r => r.ok ? r.text() : Promise.reject(new Error('HTTP ' + r.status)))
      .then(txt => {
        if (seq !== this._healthFetchSeq) return;
        if (isPoll && this._healthSig === txt) { this._updateHealthLiveCells(); return; } // unchanged — just tick
        this._healthSig = txt;
        let json = null; try { json = JSON.parse(txt); } catch (e) { json = null; }
        this._healthData = json || {};
        this._tbApplyHealth(this._healthData);
        this._healthLoaded = true;
        this.renderHealth(this._healthData);
      })
      .catch(err => {
        if (seq !== this._healthFetchSeq) return;
        if (isPoll) return; // keep current view on a transient poll failure
        this._healthLoaded = true;
        const bn = this._healthRoot && this._healthRoot.querySelector('#hl-banner');
        if (bn) bn.innerHTML = '<div class="hl-banner down"><div class="hl-bnicon">!</div><div class="hl-bntext"><div class="hl-bntitle">could not load /api/health</div><div class="hl-bnsub">' + this._galaxyEsc(err && err.message) + '</div></div></div>';
      });
  },

  renderHealth(data) {
    if (!this._healthRoot) return;
    data = data || {};
    const esc = (s) => this._galaxyEsc(s);
    const root = this._healthRoot;
    const dm = data.daemon || {};
    const t = data.totals || {};
    const stuck = Array.isArray(data.stuck) ? data.stuck : [];
    const branchLocks = Array.isArray(data.locks) ? data.locks : [];
    const resLocks = Array.isArray(data.resourceLocks) ? data.resourceLocks : [];
    const failures = Array.isArray(data.recentFailures) ? data.recentFailures : [];
    const now = Date.now();
    const DAY = 86400000;

    // Unified lock model: branch/checkout locks (carry the real release command)
    // plus non-branch resource locks (TTL'd runtime sessions etc. — shown with an
    // expiry countdown instead of a release command). Aged past 24h -> AGING, 7d -> CRITICAL.
    const allLocks = branchLocks.map(l => ({
      kind: 'branch', name: (l.branch || '?') + ' @ ' + (l.repo || '?'), owner: l.owner || '—',
      since: l.acquiredAt || 0, exp: 0,
      cmd: 'gitmoot lock release ' + (l.repo || '') + ' ' + (l.branch || '') + ' --owner ' + (l.owner || '')
    })).concat(resLocks.map(l => {
      const key = l.key || ''; const ci = key.indexOf(':');
      return { kind: ci > 0 ? key.slice(0, ci) : 'lock', name: ci > 0 ? key.slice(ci + 1) : (key || '?'),
        owner: l.owner || '—', since: l.acquiredAt || 0, exp: l.expiresAt || 0, cmd: '' };
    }));
    const agingLocks = allLocks.filter(l => l.since > 0 && (now - l.since) > DAY);

    // Vitals (computed client-side).
    const activeN = t.running || 0;
    // stuck[] already CONTAINS every blocked job (contract: blocked + queued>10m),
    // so the tile counts distinct items — adding t.blocked would double-count.
    const blockedStuckN = stuck.length;
    const lockN = allLocks.length;
    const oldestLock = allLocks.reduce((m, l) => (l.since > 0 && (m === 0 || l.since < m)) ? l.since : m, 0);
    const oldestAge = oldestLock > 0 ? (now - oldestLock) : 0;
    const totalJobs = (t.queued || 0) + (t.running || 0) + (t.blocked || 0) + (t.succeeded || 0) + (t.failed || 0) + (t.cancelled || 0);
    const failN = failures.length;
    const failRate = totalJobs > 0 ? ((t.failed || 0) / totalJobs) * 100 : 0;
    const failTone = failRate >= 10 ? '#f7768e' : failRate >= 3 ? '#e0af68' : '#9ece6a';
    const lockTone = lockN === 0 ? '#9ece6a' : oldestAge > 7 * DAY ? '#f7768e' : oldestAge > DAY ? '#e0af68' : '#7aa2f7';

    // State: down / all-clear / attention, with a manual preview override.
    const running = !!dm.running;
    const realAllClear = running && stuck.length === 0 && agingLocks.length === 0 && failRate < 3;
    const ov = this._hlOverride;
    const allClear = running && (ov === 'clear' ? true : ov === 'live' ? false : realAllClear);
    this._hlAllClear = allClear;

    const bn = root.querySelector('#hl-banner');
    if (bn) bn.innerHTML = this._hlBannerHtml(dm, {
      running: running, allClear: allClear, update: data.update,
      blocked: t.blocked || 0, stuckWorkers: stuck.filter(s => s.state !== 'blocked').length,
      aging: agingLocks.length, failures: failN, count: stuck.length + agingLocks.length
    });

    const vitals = [
      { label: 'ACTIVE', val: activeN, sub: 'jobs running', tone: '#7aa2f7' },
      { label: 'BLOCKED + STUCK', val: blockedStuckN, sub: blockedStuckN > 0 ? 'need triage' : 'all moving', tone: blockedStuckN > 0 ? '#e0af68' : '#9ece6a' },
      { label: 'LOCKS', val: lockN, sub: lockN > 0 ? ('oldest ' + this._hlAgeShort(oldestAge)) : 'none held', tone: lockTone },
      { label: 'FAILURES · recent', val: failN, sub: failRate.toFixed(1) + '% overall rate', tone: failN === 0 ? '#9ece6a' : failTone }
    ];
    const vel = root.querySelector('#hl-vitals');
    if (vel) vel.innerHTML = vitals.map(v => '<div class="hl-tile"><span class="hl-tlabel">' + esc(v.label) + '</span><span class="hl-tval" style="color:' + v.tone + '">' + esc(String(v.val)) + '</span><span class="hl-tsub">' + esc(v.sub) + '</span></div>').join('');

    // De-emphasized secondary totals (succeeded / cancelled / queued / running + version).
    const sec = root.querySelector('#hl-second');
    if (sec) {
      const verRaw = dm.version || '';
      const ver = verRaw ? (/^v/i.test(verRaw) ? verRaw : 'v' + verRaw) : '';
      const parts = [];
      if (ver) parts.push(esc(ver));
      parts.push('<b>' + (t.succeeded || 0) + '</b> succeeded');
      parts.push('<b>' + (t.cancelled || 0) + '</b> cancelled');
      parts.push('<b>' + (t.queued || 0) + '</b> queued');
      parts.push('<b>' + (t.running || 0) + '</b> running');
      sec.innerHTML = parts.join('<span style="color:#3a3f5a">&nbsp;·&nbsp;</span>');
    }

    // Detail sections collapse in the all-clear state.
    const stuckSec = root.querySelector('#hl-stuck-sec');
    const locksSec = root.querySelector('#hl-locks-sec');
    if (stuckSec) stuckSec.style.display = allClear ? 'none' : 'block';
    if (locksSec) locksSec.style.display = allClear ? 'none' : 'block';

    const sc = root.querySelector('#hl-stuck-c'); if (sc) sc.textContent = '(' + stuck.length + ')';
    const sEl = root.querySelector('#hl-stuck');
    if (sEl) sEl.innerHTML = stuck.length ? '<div class="hl-list">' + stuck.map(j => this._hlStuckRow(j)).join('') + '</div>' : '<div class="hl-empty">nothing stuck <span class="hl-ok">✓</span></div>';
    const lEl = root.querySelector('#hl-locks');
    if (lEl) lEl.innerHTML = allLocks.length
      ? '<div class="hl-list">' + allLocks.map(l => { const age = l.since > 0 ? (now - l.since) : 0; const tone = age > 7 * DAY ? '#f7768e' : age > DAY ? '#e0af68' : '#5a6088'; const lvl = age > 7 * DAY ? 'critical' : age > DAY ? 'aging' : ''; return this._hlLockRow(l, tone, lvl); }).join('') + '</div>'
      : '<div class="hl-empty">no locks held <span class="hl-ok">✓</span></div>';

    this._updateHealthLiveCells();
  },

  // "11d" / "3h" / "8m" — a compact age label from a duration in ms.
  _hlAgeShort(ms) {
    if (!ms || ms <= 0) return '0m';
    let s = Math.floor(ms / 1000);
    const d = Math.floor(s / 86400); if (d > 0) return d + 'd';
    const h = Math.floor(s / 3600); if (h > 0) return h + 'h';
    return Math.max(1, Math.floor(s / 60)) + 'm';
  },

  // #rrggbb -> rgba(r,g,b,a) for chip tints (avoids color-mix for older engines).
  _hlTint(hex, a) {
    const m = /^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(String(hex));
    if (!m) return 'rgba(160,168,220,' + a + ')';
    return 'rgba(' + parseInt(m[1], 16) + ',' + parseInt(m[2], 16) + ',' + parseInt(m[3], 16) + ',' + a + ')';
  },

  _hlBannerHtml(dm, s) {
    const esc = (x) => this._galaxyEsc(x);
    const running = !!s.running;
    const upSpan = (running && dm.startedAt > 0) ? '<span id="hl-uptime" data-started="' + dm.startedAt + '"></span>' : '';
    const upSeg = upSpan ? ('up ' + upSpan) : 'running';
    const verSeg = dm.version ? (' · ' + esc(/^v/i.test(dm.version) ? dm.version : 'v' + dm.version)) : '';
    // Update badge (fail-open: nothing when health.update is absent / up to date).
    let updHtml = '';
    const upd = s.update;
    if (upd && upd.updateAvailable) {
      const label = '↑ update' + (upd.latest ? ' ' + upd.latest : ' available');
      const href = this._galaxySafeUrl(upd.releaseUrl);
      updHtml = href
        ? '<a class="hl-upd" href="' + esc(upd.releaseUrl) + '" target="_blank" rel="noopener" title="update available">' + esc(label) + ' <span style="color:#c99a4e">↗</span></a>'
        : '<span class="hl-upd" title="update available">' + esc(label) + '</span>';
    }
    if (!running) {
      return '<div class="hl-banner down">' +
        '<div class="hl-bnicon">✕</div>' +
        '<div class="hl-bntext"><div class="hl-bntitle">Daemon stopped — orchestration paused</div>' +
        '<div class="hl-bnsub">no heartbeat · workers idle' + verSeg + '</div></div>' +
        (updHtml ? '<div class="hl-bnright">' + updHtml + '</div>' : '') + '</div>';
    }
    if (s.allClear) {
      return '<div class="hl-banner ok">' +
        '<div class="hl-bnicon">✓</div>' +
        '<div class="hl-bntext"><div class="hl-bntitle">All clear — daemon healthy</div>' +
        '<div class="hl-bnsub">' + upSeg + ' · no stuck jobs · no aging locks · failures within threshold</div></div>' +
        '<div class="hl-bnright">' + updHtml + '<button class="hl-toggle" data-hl-toggle="1" type="button">view live state ▸</button></div></div>';
    }
    const n = s.count;
    const noun = n === 1 ? 'item needs' : 'items need';
    const sub = upSeg + ' · ' + s.blocked + ' blocked · ' + s.stuckWorkers + ' stuck · ' + s.aging + ' locks aging · ' + s.failures + ' recent failures';
    return '<div class="hl-banner attention">' +
      '<div class="hl-bnicon">⚠</div>' +
      '<div class="hl-bntext"><div class="hl-bntitle">Daemon healthy — ' + n + ' ' + noun + ' your attention</div>' +
      '<div class="hl-bnsub">' + sub + '</div></div>' +
      '<div class="hl-bnright">' + updHtml + '<button class="hl-toggle" data-hl-toggle="1" type="button">preview all-clear ▸</button></div></div>';
  },

  // Compact one-line stuck-job row: dot . bold title . agent chip . repo . age . why >.
  // The row (minus the agent chip / why button) opens the in-place job detail.
  _hlStuckRow(j) {
    const esc = (s) => this._galaxyEsc(s);
    const state = j.state || 'blocked';
    const col = this._galaxyStateColor(state);
    const dot = '<span class="hl-dot" style="background:' + col + ';box-shadow:0 0 8px ' + col + (state === 'running' ? ';animation:dotpulse 1.4s ease-in-out infinite' : '') + '"></span>';
    const ag = j.agent ? '<button class="hl-agchip" data-agent="' + esc(j.agent) + '" type="button" title="View ' + esc(j.agent) + '">' + esc(j.agent) + '</button>' : '';
    const repo = j.repo ? '<span class="hl-srepo">' + esc(j.repo) + '</span>' : '';
    const why = j.reason ? '<button class="hl-why" data-hl-why="1" type="button">why <span class="hl-chev">▸</span></button>' : '';
    const body = j.reason ? '<div class="hl-whybody" hidden>' + esc(j.reason) + '</div>' : '';
    return '<div class="hl-srow" data-id="' + esc(j.id) + '">' +
      '<div class="hl-sline">' + dot +
        '<span class="hl-stitle">' + esc(j.title || j.id) + '</span>' + ag + repo +
        '<span class="hl-grow"></span>' +
        '<span class="hl-sage hl-age" data-since="' + (j.since || 0) + '">—</span>' + why +
      '</div>' + body + '</div>';
  },

  // Lock row: dot . kind.name . held by X . AGING/CRITICAL chip . age . release cmd
  // (branch locks) or an expiry countdown (TTL'd resource locks).
  _hlLockRow(l, tone, lvl) {
    const esc = (s) => this._galaxyEsc(s);
    const dot = '<span class="hl-dot" style="background:' + tone + ';box-shadow:0 0 8px ' + tone + '"></span>';
    const chip = lvl ? '<span class="hl-esc" style="color:' + tone + ';background:' + this._hlTint(tone, 0.12) + ';border:1px solid ' + this._hlTint(tone, 0.28) + '">' + lvl + '</span>' : '';
    const age = '<span class="hl-lage hl-age" data-since="' + (l.since || 0) + '" style="color:' + tone + '">—</span>';
    const tail = l.cmd
      ? '<div class="hl-cmd"><code>' + esc(l.cmd) + '</code><button class="gm-copy" data-cmd="' + esc(l.cmd) + '" type="button">copy</button></div>'
      : '<div class="hl-exppill">ttl <span class="hl-exp" data-exp="' + (l.exp || 0) + '">—</span></div>';
    return '<div class="hl-lrow">' + dot +
      '<span class="hl-lname"><span class="hl-lkind">' + esc(l.kind) + '</span> · ' + esc(l.name) + '</span>' +
      '<span class="hl-lheld">held by ' + esc(l.owner) + '</span>' + chip +
      '<span class="hl-grow"></span>' + age + tail + '</div>';
  },

  // Toggle a stuck row's "why" reason body + chevron.
  _hlToggleWhy(btn) {
    const row = btn.closest('.hl-srow'); if (!row) return;
    const body = row.querySelector('.hl-whybody'); if (!body) return;
    const chev = btn.querySelector('.hl-chev');
    if (body.hasAttribute('hidden')) { body.removeAttribute('hidden'); if (chev) chev.textContent = '▾'; }
    else { body.setAttribute('hidden', ''); if (chev) chev.textContent = '▸'; }
  },

  // Navigate to /agents/<name> and open that agent's detail panel (house cross-link law).
  _hlGotoAgent(name) {
    if (!name) return;
    this._pendingAgentDetail = name;
    this.setState({ view: 'agents' });
    this._navRoute('agents', { agent: name });
  },

  _updateHealthLiveCells() {
    const root = this._healthRoot; if (!root) return;
    const up = root.querySelector('#hl-uptime');
    if (up) { const s = +up.getAttribute('data-started'); up.textContent = s > 0 ? this._fmtUptime(Date.now() - s) : ''; }
    root.querySelectorAll('.hl-age').forEach(el => { const s = +el.getAttribute('data-since'); el.textContent = s > 0 ? this._relFromMs(s).replace(/ ago$/, '') : '—'; });
    root.querySelectorAll('.hl-exp').forEach(el => { const s = +el.getAttribute('data-exp'); el.textContent = s > 0 ? this._fmtCountdown(s - Date.now()) : '—'; });
  },

  _healthJobSummary(id) {
    const d = this._healthData || {};
    const all = (d.stuck || []).concat(d.recentFailures || []);
    const m = all.find(x => x.id === id);
    return m ? { id: m.id, title: m.title, agent: m.agent, repo: m.repo, state: m.state } : null;
  },

  ensureHealthDetailEl() {
    if (this._healthDetailEl && this._healthDetailEl.parentNode) return this._healthDetailEl;
    const el = document.createElement('div');
    el.className = 'xd-panel';
    this._healthRoot.appendChild(el);
    this._healthDetailEl = el;
    return el;
  },
  closeHealthDetail() {
    this._healthSelected = null;
    this._healthDetailFetchId = null;
    try { if (this._healthDetailEl) this._healthDetailEl.remove(); } catch (e) {}
    this._healthDetailEl = null;
  },
  openHealthDetail(id) {
    if (!id) { this.closeHealthDetail(); return; }
    this._ensureDetailStyles();
    this._healthSelected = id;
    const summary = this._healthJobSummary(id) || { id: id };
    this.renderHealthDetail(summary, null, 'loading');
    const fid = (this._healthDetailFetchId = id);
    fetch('/api/job/' + encodeURIComponent(id), { cache: 'no-store' })
      .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
      .then(job => { if (this._healthDetailFetchId === fid) this.renderHealthDetail(summary, job || {}, 'ok'); })
      .catch(() => { if (this._healthDetailFetchId === fid) this.renderHealthDetail(summary, null, 'error'); });
  },
  renderHealthDetail(summary, job, status) {
    const el = this.ensureHealthDetailEl();
    el.innerHTML = this._jobDetailHTML(summary, job, status);
    const c = el.querySelector('.xd-close');
    if (c) c.addEventListener('click', () => this.closeHealthDetail());
  },

};
