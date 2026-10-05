'use strict';
const el = id => document.getElementById(id);
let snapshot = null, selected = '', busy = false, renderedModel = '', renderedID = '', renderedManufacturer = '';
const pretty = value => JSON.stringify(value || {}, null, 2);
function message(text, error = false) { el('message').textContent = text; el('message').className = error ? 'error' : 'success'; }
async function api(path, body) {
  const headers = {}; if (body !== undefined) headers['Content-Type'] = 'application/json';
  const response = await fetch(path, {method: body === undefined ? 'GET' : 'POST', headers, body: body === undefined ? undefined : JSON.stringify(body)});
  const text = await response.text(); let data; try { data = JSON.parse(text); } catch { data = {error: text}; }
  if (!response.ok || data.status === 'error') throw new Error(data.error || 'HTTP ' + response.status); return data;
}
function current() { return snapshot?.devices.find(d => d.ieee_address === selected); }
async function refresh() {
  snapshot = await api('/api/admin'); el('version').textContent = snapshot.version;
  const h = snapshot.health; el('health').textContent = 'MQTT: ' + (h.mqtt_ready ? 'подключён' : 'нет связи') + ' · Координатор: ' + (h.coordinator_connected ? 'подключён' : 'нет связи');
  el('joinStatus').textContent = h.permit_join ? 'Открыто, осталось ' + h.permit_join_remaining + ' с' : 'Закрыто'; el('devices').replaceChildren();
  for (const d of snapshot.devices) {
    const tr = document.createElement('tr');
    for (const value of [d.friendly_name, d.model_id || 'Модель ещё не определена', d.manufacturer || '—', d.ieee_address, d.last_seen && !d.last_seen.startsWith('0001') ? new Date(d.last_seen).toLocaleString() : '—']) { const td = document.createElement('td'); td.textContent = value; tr.append(td); }
    tr.addEventListener('click', () => { selected = d.ieee_address; renderDevice(true); }); el('devices').append(tr);
  }
  if (selected && !current()) selected = ''; renderDevice(false);
}
function renderDevice(reset) {
  const d = current(); el('devicePanel').hidden = !d; if (!d) { renderedID = ''; return; } el('deviceTitle').textContent = d.friendly_name; el('state').textContent = pretty(snapshot.states[d.ieee_address]);
  const previousChoice = el('replacement').value;
  el('replacement').replaceChildren();
  for (const next of snapshot.devices.filter(x => x.ieee_address !== selected && x.model_id && x.model_id === d.model_id && (!['TS0601', 'TS011F'].includes(d.model_id) || (d.manufacturer && x.manufacturer === d.manufacturer)))) { const opt = document.createElement('option'); opt.value = next.ieee_address; opt.textContent = next.friendly_name + ' · ' + next.ieee_address; el('replacement').append(opt); }
  if ([...el('replacement').options].some(x => x.value === previousChoice)) el('replacement').value = previousChoice;
  el('replace').disabled = el('replacement').options.length === 0;
  if (!reset && renderedID === d.ieee_address && renderedModel === d.model_id && renderedManufacturer === d.manufacturer) return;
  renderedID = d.ieee_address; renderedModel = d.model_id; renderedManufacturer = d.manufacturer;
  const definition = snapshot.definitions[d.ieee_address] || {};
  const properties = definition.properties || {};
  const siren = properties.alarm?.type === 'boolean' && properties.alarm.access.includes('w');
  el('name').value = d.friendly_name; el('options').value = pretty(d.options); el('payload').value = '{}'; el('force').checked = false; el('channels').replaceChildren();
  const channels = snapshot.channels[d.ieee_address] || {};
  for (const channel of Object.keys(channels)) for (const state of ['ON', 'OFF']) { const btn = document.createElement('button'); btn.textContent = channel + ': ' + state; btn.addEventListener('click', () => operate('device/set', {payload: {['state_' + channel]: state}})); el('channels').append(btn); }
  const stateWritable = definition.id === 'standard_zcl' || definition.protocol === 'builtin' || properties.state?.access?.includes('w');
  if (stateWritable && !siren && Object.keys(channels).length === 0 && !['lumi.weather', 'lumi.sensor_wleak.aq1', 'lumi.remote.b186acn02'].includes(d.model_id) && d.endpoints.filter(ep => ep.profileID === 260 && ep.inputClusters?.includes(6)).length === 1) {
    for (const state of ['ON', 'OFF']) { const btn = document.createElement('button'); btn.textContent = state; btn.addEventListener('click', () => operate('device/set', {payload: {state}})); el('channels').append(btn); }
  }
  if (siren) for (const alarm of [true, false]) { const btn = document.createElement('button'); btn.textContent = alarm ? 'Включить сирену' : 'Выключить сирену'; btn.addEventListener('click', () => operate('device/set', {payload: {alarm}})); el('channels').append(btn); }
  el('configure').disabled = !definition.configurable;
  el('parameterHelp').textContent = d.model_id === 'lumi.switch.b2nc01' ? 'state_left/right: ON/OFF/TOGGLE; operation_mode_left/right: control_relay/decoupled; power_outage_memory: true/false; flip_indicator_light: ON/OFF.' : d.model_id === 'lumi.relay.c2acn01' ? 'state_l1/l2: ON/OFF/TOGGLE; interlock: true/false; power_outage_memory: true/false. /get: state_l1, state_l2, power.' : d.model_id === 'TS011F' ? 'state: ON/OFF/TOGGLE; power_outage_memory: off/on/restore (строка); power_on_behavior: off/on/previous; /get: energy, power, voltage, current. Пример: {"state":"OFF","power_outage_memory":"restore"}.' : d.model_id === 'lumi.weather' ? 'Публикуются temperature, humidity, pressure, battery и voltage; датчик спит между отчётами.' : d.model_id === 'lumi.remote.b186acn02' ? 'Публикуются события action: single/double/hold/triple/release. Команды state для этой кнопки не применяются.' : d.model_id === 'lumi.sensor_wleak.aq1' ? 'Публикуются water_leak, battery_low, battery, voltage. Если last_seen отсутствует, входящий кадр ещё не получен.' : siren ? 'NEO NAS-AB02B2: alarm: true/false; melody: 1..18 (число или строка); volume: low/medium/high; duration: 0..1800 секунд. Пример: {"melody":"7","volume":"high","duration":60,"alarm":true}. /get запрашивает весь набор Tuya DP, наличие ответа зависит от устройства.' : d.model_id === 'TS0601' ? 'Вариант определяется по производителю. Известные климатические датчики дают temperature/humidity; прочие показывают сырые tuya_datapoints. Для диагностики скачайте список моделей.' : 'Стандартные /set: state, brightness, color_temp, transition. Особенности этой модели могут быть не реализованы.';
  if (definition.description) el('parameterHelp').textContent = definition.description;
  else if (Object.keys(properties).length) el('parameterHelp').textContent = definition.id + ': ' + Object.entries(properties).map(([name, p]) => name + ' [' + p.access + ', ' + p.type + ']' + (p.values ? ': ' + Object.keys(p.values).join('/') : '') + (p.min !== undefined || p.max !== undefined ? ' диапазон ' + (p.min ?? '—') + '..' + (p.max ?? '—') : '')).join('; ');
}
async function operate(request, extra = {}, ask = '') {
  if (busy || (ask && !confirm(ask))) return; busy = true;
  try { message('Выполняется…'); const result = await api('/api/request', {request, data: {id: selected, ...extra}}); if (request === 'device/replace') selected = result.data.device.ieee_address; if (request === 'device/remove') selected = ''; await refresh(); renderDevice(true); message(result.data.warning || (result.data.transport_accepted ? 'Команда принята транспортом. Ожидается ответ устройства.' : result.data.interview_queued ? 'Опрос поставлен в очередь. Спящее устройство разбудите кнопкой.' : 'Сохранено.')); } catch (error) { message(error.message, true); } finally { busy = false; }
}
function jsonInput(id) { const value = JSON.parse(el(id).value); if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Требуется JSON-объект'); return value; }
function action(id, handler) { el(id).addEventListener('click', async () => { try { await handler(); } catch (error) { message(error.message, true); } }); }
action('refresh', refresh);
action('loadDefinitions', async () => { const data = await api('/api/definitions'); el('definitionsJSON').value = pretty(data); el('definitionsPanel').hidden = false; });
action('reloadDefinitions', () => operate('definitions/reload'));
action('saveDefinitions', () => operate('definitions/save', {definitions: jsonInput('definitionsJSON')}));
action('joinOpen', () => operate('permit_join', {time: Number(el('joinTime').value)})); action('joinClose', () => operate('permit_join', {time: 0}));
action('rename', () => operate('device/rename', {to: el('name').value})); action('saveOptions', () => operate('device/options', {options: jsonInput('options')}));
action('set', () => operate('device/set', {payload: jsonInput('payload')})); action('get', () => operate('device/get', {payload: jsonInput('payload')})); action('configure', () => operate('device/configure'));
action('interview', () => operate('device/interview'));
action('replace', () => operate('device/replace', {to: el('replacement').value}, 'Сохранить MQTT-имя за новым устройством и забыть старое?'));
action('remove', () => operate('device/remove', {force: el('force').checked}, 'Удалить ' + current()?.friendly_name + '?'));
action('export', () => { if (!snapshot) throw new Error('Сначала подключитесь'); const link = document.createElement('a'); const url = URL.createObjectURL(new Blob([pretty(snapshot.devices)], {type: 'application/json'})); link.href = url; link.download = 'devices-diagnostic.json'; link.click(); URL.revokeObjectURL(url); });
refresh().catch(error => message('Ошибка подключения: ' + error.message, true));
setInterval(() => { if (snapshot && !busy) refresh().catch(error => message(error.message, true)); }, 5000);
