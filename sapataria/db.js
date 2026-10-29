// ==================================================
// IndexedDB REPOSITORY & CLOUD SYNC ENGINE
// Sapataria Calce Couro - Sincronização em Tempo Real
// Integrado com Google Apps Script / TaskFlow
// ==================================================

const DB_NAME = 'CalceCouroDB';
const DB_VERSION = 2;
let dbInstance = null;

// URL Oficial do Web App Google Apps Script conectado à Planilha
const APPS_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbzlmXyjnVs5uH41a_9bddnNfIn-M2KMU4arwHreAlRkB0DFw3ziMLeR5MOVRarQy_4r/exec';

// ==================================================
// 1. INDEXEDDB LOCAL-FIRST
// ==================================================
function openDB() {
  return new Promise((resolve, reject) => {
    if (dbInstance) return resolve(dbInstance);

    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;

      // Store para Ordens de Serviço
      if (!db.objectStoreNames.contains('ordens_servico')) {
        const osStore = db.createObjectStore('ordens_servico', { keyPath: 'id', autoIncrement: true });
        osStore.createIndex('numero_os', 'numero_os', { unique: true });
        osStore.createIndex('status', 'status', { unique: false });
        osStore.createIndex('cliente_nome', 'cliente_nome', { unique: false });
        osStore.createIndex('data_entrada', 'data_entrada', { unique: false });
        osStore.createIndex('sincronizado', 'sincronizado', { unique: false });
      }

      // Store para Configurações / Contadores / Metadados de Sync
      if (!db.objectStoreNames.contains('configuracoes')) {
        db.createObjectStore('configuracoes', { keyPath: 'chave' });
      }
    };

    request.onsuccess = (event) => {
      dbInstance = event.target.result;
      resolve(dbInstance);
    };

    request.onerror = (event) => {
      console.error('Erro ao abrir IndexedDB:', event.target.error);
      reject(event.target.error);
    };
  });
}

// Obter próximo número sequencial de OS (#1, #2...)
async function getNextOSNumber() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('ordens_servico', 'readonly');
    const store = tx.objectStore('ordens_servico');
    const countReq = store.count();

    countReq.onsuccess = () => {
      if (countReq.result === 0) {
        resolve(1);
      } else {
        const cursorReq = store.openCursor(null, 'prev');
        cursorReq.onsuccess = (e) => {
          const cursor = e.target.result;
          if (cursor && cursor.value && cursor.value.numero_os) {
            resolve(cursor.value.numero_os + 1);
          } else {
            resolve(countReq.result + 1);
          }
        };
        cursorReq.onerror = () => resolve(countReq.result + 1);
      }
    };

    countReq.onerror = () => reject(countReq.error);
  });
}

// Salvar / Atualizar OS Localmente e Marcar para Sincronização
async function salvarOS(os, salvarNaFilha = true) {
  const db = await openDB();
  return new Promise(async (resolve, reject) => {
    try {
      if (!os.numero_os) {
        os.numero_os = await getNextOSNumber();
      }
      if (!os.data_entrada) {
        os.data_entrada = new Date().toISOString();
      }

      const agora = new Date().toISOString();
      os.atualizado_em = agora;
      os.sincronizado = false;

      // Calcula financeiro
      const totalItens = (os.itens || []).reduce((acc, it) => acc + (parseFloat(it.valor) || 0), 0);
      os.valor_total = totalItens;
      os.valor_entrada = parseFloat(os.valor_entrada) || 0;
      os.valor_restante = Math.max(0, os.valor_total - os.valor_entrada);

      const tx = db.transaction('ordens_servico', 'readwrite');
      const store = tx.objectStore('ordens_servico');
      const request = os.id ? store.put(os) : store.add(os);

      request.onsuccess = (e) => {
        os.id = os.id || e.target.result;
        resolve(os);

        // Dispara sync na nuvem em segundo plano sem travar a interface
        if (salvarNaFilha) {
          CloudSync.pushOrder(os).catch(err => console.warn('Sync em fila offline:', err));
        }
      };

      request.onerror = () => reject(request.error);
    } catch (err) {
      reject(err);
    }
  });
}

// Listar todas as ordens locais
async function listarTodasOS() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('ordens_servico', 'readonly');
    const store = tx.objectStore('ordens_servico');
    const request = store.getAll();

    request.onsuccess = () => {
      const ordens = request.result || [];
      ordens.sort((a, b) => (b.numero_os || b.id) - (a.numero_os || a.id));
      resolve(ordens);
    };

    request.onerror = () => reject(request.error);
  });
}

// Obter OS por ID
async function obterOSPorId(id) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('ordens_servico', 'readonly');
    const store = tx.objectStore('ordens_servico');
    const request = store.get(Number(id));

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

// Excluir OS
async function excluirOS(id) {
  const os = await obterOSPorId(id);
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('ordens_servico', 'readwrite');
    const store = tx.objectStore('ordens_servico');
    const request = store.delete(Number(id));

    request.onsuccess = () => {
      resolve(true);
      if (os && os.numero_os) {
        CloudSync.deleteOrder(os.numero_os).catch(err => console.warn('Erro ao excluir na nuvem:', err));
      }
    };
    request.onerror = () => reject(request.error);
  });
}

// Exportar Backup Completo JSON
async function exportarBackupJSON() {
  const ordens = await listarTodasOS();
  const backup = {
    versao: '2.0',
    data_backup: new Date().toISOString(),
    empresa: 'Sapataria Calce Couro',
    total_ordens: ordens.length,
    ordens: ordens
  };

  const blob = new Blob([JSON.stringify(backup, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const dataFormatada = new Date().toISOString().split('T')[0];
  a.download = `backup_calce_couro_${dataFormatada}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

// Importar Backup Completo JSON
async function importarBackupJSON(arquivoJson) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = async (e) => {
      try {
        const dados = JSON.parse(e.target.result);
        if (!dados.ordens || !Array.isArray(dados.ordens)) {
          throw new Error('Formato de arquivo de backup inválido.');
        }

        const db = await openDB();
        const tx = db.transaction('ordens_servico', 'readwrite');
        const store = tx.objectStore('ordens_servico');

        let importadas = 0;
        for (const item of dados.ordens) {
          delete item.id;
          item.sincronizado = false;
          store.put(item);
          importadas++;
        }

        tx.oncomplete = async () => {
          // Força envio para a nuvem
          CloudSync.sync().catch(console.error);
          resolve(importadas);
        };
        tx.onerror = () => reject(tx.error);
      } catch (err) {
        reject(err);
      }
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsText(arquivoJson);
  });
}

// ==================================================
// 2. CLOUD SYNC ENGINE (GOOGLE APPS SCRIPT & TASKFLOW)
// ==================================================
const CloudSync = {
  status: 'idle', // 'idle' | 'syncing' | 'synced' | 'offline' | 'error'
  lastSyncTime: null,
  syncListeners: [],
  autoSyncTimer: null,
  isSyncing: false,

  onStatusChange(fn) {
    if (typeof fn === 'function') this.syncListeners.push(fn);
  },

  notifyStatus(status, details = null) {
    this.status = status;
    this.syncListeners.forEach(fn => {
      try { fn(status, details); } catch(e) {}
    });
  },

  // Executa comando no Apps Script do Google
  async executeCloud(code) {
    const res = await fetch(APPS_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'execute_command', code: code })
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    const data = await res.json();
    if (!data.success) throw new Error(data.error || 'Erro no script remoto');
    return data.result;
  },

  // Salva ou atualiza uma OS na nuvem
  async pushOrder(os, autor) {
    autor = autor || localStorage.getItem('calce_operador_nome') || 'Oficina (Pais)';
    const code = `
    (function() {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheetByName('Sapataria_Ordens');
      if (!sheet) {
        sheet = ss.insertSheet('Sapataria_Ordens');
        sheet.getRange(1, 1, 1, 14).setValues([[
          'ID', 'Data Entrada', 'Cliente', 'Telefone', 'Itens',
          'Previsao Retirada', 'Valor Total', 'Entrada Paga', 'Restante',
          'Status', 'Observacoes', 'Foto', 'Ultima Atualizacao', 'SincronizadoPor'
        ]]).setFontWeight('bold').setBackground('#2b1d1a').setFontColor('#ffffff');
        sheet.setFrozenRows(1);
      }

      var os = ${JSON.stringify(os)};
      var autor = ${JSON.stringify(autor)};
      var numOS = Number(os.numero_os);
      var data = sheet.getDataRange().getValues();
      var rowIndex = -1;

      for (var i = 1; i < data.length; i++) {
        if (Number(data[i][0]) === numOS) {
          rowIndex = i + 1;
          break;
        }
      }

      var previsaoCompleta = (os.previsao_retirada_data || '') + (os.previsao_retirada_hora ? ' às ' + os.previsao_retirada_hora : '');
      var itensStr = JSON.stringify(os.itens || []);
      var agora = new Date().toISOString();

      var rowValues = [
        numOS,
        os.data_entrada || agora,
        os.cliente_nome || '',
        os.cliente_whatsapp || '',
        itensStr,
        previsaoCompleta,
        Number(os.valor_total) || 0,
        Number(os.valor_entrada) || 0,
        Number(os.valor_restante) || 0,
        os.status || 'Aguardando Início',
        os.observacoes || '',
        os.foto_base64 ? (os.foto_base64.length > 500 ? '[FOTO_SALVA]' : os.foto_base64) : '',
        agora,
        autor
      ];

      if (rowIndex > 0) {
        sheet.getRange(rowIndex, 1, 1, 14).setValues([rowValues]);
      } else {
        sheet.appendRow(rowValues);
      }
      return { success: true, numero_os: numOS, atualizado_em: agora };
    })()
    `;

    try {
      this.notifyStatus('syncing');
      const res = await this.executeCloud(code);
      this.lastSyncTime = new Date();
      this.notifyStatus('synced', { numero_os: os.numero_os });
      return res;
    } catch (err) {
      this.notifyStatus('offline', { error: err.message });
      throw err;
    }
  },

  // Atualiza apenas status de uma OS
  async updateStatus(numero_os, novoStatus, autor) {
    autor = autor || localStorage.getItem('calce_operador_nome') || 'Oficina (Pais)';
    const code = `
    (function() {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheetByName('Sapataria_Ordens');
      if (!sheet) return { error: 'Aba nao encontrada' };

      var data = sheet.getDataRange().getValues();
      var numOS = Number(${Number(numero_os)});
      var agora = new Date().toISOString();
      for (var i = 1; i < data.length; i++) {
        if (Number(data[i][0]) === numOS) {
          sheet.getRange(i + 1, 10).setValue(${JSON.stringify(novoStatus)});
          sheet.getRange(i + 1, 13).setValue(agora);
          sheet.getRange(i + 1, 14).setValue(${JSON.stringify(autor)});
          return { success: true, numero_os: numOS, status: ${JSON.stringify(novoStatus)}, atualizado_em: agora };
        }
      }
      return { error: 'OS nao encontrada' };
    })()
    `;

    try {
      this.notifyStatus('syncing');
      const res = await this.executeCloud(code);
      this.lastSyncTime = new Date();
      this.notifyStatus('synced');
      return res;
    } catch (err) {
      this.notifyStatus('offline', { error: err.message });
      throw err;
    }
  },

  // Deleta OS na nuvem
  async deleteOrder(numero_os) {
    const code = `
    (function() {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheetByName('Sapataria_Ordens');
      if (!sheet) return { error: 'Aba nao encontrada' };

      var data = sheet.getDataRange().getValues();
      var numOS = Number(${Number(numero_os)});
      for (var i = 1; i < data.length; i++) {
        if (Number(data[i][0]) === numOS) {
          sheet.deleteRow(i + 1);
          return { success: true, deleted: numOS };
        }
      }
      return { error: 'OS nao encontrada' };
    })()
    `;

    try {
      this.notifyStatus('syncing');
      await this.executeCloud(code);
      this.lastSyncTime = new Date();
      this.notifyStatus('synced');
      return true;
    } catch (err) {
      this.notifyStatus('offline', { error: err.message });
      throw err;
    }
  },

  // Sincronização Bidirecional Completa (Pull + Push)
  async sync() {
    if (this.isSyncing) return;
    this.isSyncing = true;
    this.notifyStatus('syncing');

    try {
      const codePull = `
      (function() {
        var ss = SpreadsheetApp.getActiveSpreadsheet();
        var sheet = ss.getSheetByName('Sapataria_Ordens');
        if (!sheet) return [];
        var data = sheet.getDataRange().getValues();
        if (data.length <= 1) return [];
        var list = [];
        for (var i = 1; i < data.length; i++) {
          var row = data[i];
          if (!row[0] && !row[2]) continue;
          var itens = [];
          try {
            itens = typeof row[4] === 'string' && row[4].startsWith('[') ? JSON.parse(row[4]) : [{ nome: 'Conserto', servico: String(row[4] || ''), valor: Number(row[6]) || 0 }];
          } catch(e) {
            itens = [{ nome: 'Serviço', servico: String(row[4] || ''), valor: Number(row[6]) || 0 }];
          }
          
          var prevParts = String(row[5] || '').split(' às ');
          var prevData = prevParts[0] || '';
          var prevHora = prevParts[1] || '';

          list.push({
            numero_os: Number(row[0]) || i,
            data_entrada: String(row[1] || ''),
            cliente_nome: String(row[2] || ''),
            cliente_whatsapp: String(row[3] || ''),
            itens: itens,
            previsao_retirada_data: prevData,
            previsao_retirada_hora: prevHora,
            valor_total: Number(row[6]) || 0,
            valor_entrada: Number(row[7]) || 0,
            valor_restante: Number(row[8]) || 0,
            status: String(row[9] || 'Aguardando Início'),
            observacoes: String(row[10] || ''),
            foto_base64: String(row[11] || ''),
            atualizado_em: String(row[12] || ''),
            sincronizado_por: String(row[13] || '')
          });
        }
        return list;
      })()
      `;

      const remoteOrders = await this.executeCloud(codePull);
      const localOrders = await listarTodasOS();

      const db = await openDB();
      const tx = db.transaction('ordens_servico', 'readwrite');
      const store = tx.objectStore('ordens_servico');

      const localMap = new Map();
      localOrders.forEach(o => localMap.set(Number(o.numero_os), o));

      let alterouLocal = false;
      const pendentesPush = [];

      // 1. Processa dados da nuvem para o local
      for (const rem of remoteOrders) {
        const loc = localMap.get(Number(rem.numero_os));
        if (!loc) {
          // Nova ordem criada por outro aparelho (ex: filho pelo PC ou pai pelo celular)
          rem.sincronizado = true;
          store.add(rem);
          alterouLocal = true;
        } else {
          const remTime = new Date(rem.atualizado_em || 0).getTime();
          const locTime = new Date(loc.atualizado_em || 0).getTime();

          if (remTime > locTime) {
            // Nuvem tem dados mais recentes
            rem.id = loc.id;
            rem.sincronizado = true;
            store.put(rem);
            alterouLocal = true;
          } else if (locTime > remTime && loc.sincronizado === false) {
            // Local tem alteração não enviada
            pendentesPush.push(loc);
          }
        }
        localMap.delete(Number(rem.numero_os));
      }

      // 2. Ordens locais que ainda não estão na nuvem
      for (const loc of localMap.values()) {
        pendentesPush.push(loc);
      }

      await new Promise((res, rej) => {
        tx.oncomplete = res;
        tx.onerror = rej;
      });

      // 3. Envia pendências locais para a nuvem
      for (const pend of pendentesPush) {
        try {
          await this.pushOrder(pend);
          pend.sincronizado = true;
          const txUp = db.transaction('ordens_servico', 'readwrite');
          txUp.objectStore('ordens_servico').put(pend);
        } catch(e) {
          console.warn('Erro ao subir pendência:', e);
        }
      }

      this.lastSyncTime = new Date();
      this.notifyStatus('synced', { alterouLocal, total: remoteOrders.length });
      return { alterouLocal, total: remoteOrders.length };
    } catch (err) {
      console.warn('Erro no sync bidirecional:', err);
      this.notifyStatus('offline', { error: err.message });
      return { alterouLocal: false, error: err.message };
    } finally {
      this.isSyncing = false;
    }
  },

  // Inicia Polling Automático em segundo plano
  startAutoSync(intervalSeconds = 10, onUpdateCallback = null) {
    if (this.autoSyncTimer) clearInterval(this.autoSyncTimer);

    // Sync inicial imediato
    this.sync().then(res => {
      if (res && res.alterouLocal && typeof onUpdateCallback === 'function') {
        onUpdateCallback();
      }
    });

    this.autoSyncTimer = setInterval(async () => {
      // Só roda se a página estiver visível (economiza bateria dos celulares)
      if (document.hidden) return;

      const res = await this.sync();
      if (res && res.alterouLocal && typeof onUpdateCallback === 'function') {
        onUpdateCallback();
      }
    }, intervalSeconds * 1000);

    // Quando o usuário volta à aba do navegador, sincroniza na hora
    window.addEventListener('visibilitychange', () => {
      if (!document.hidden) {
        this.sync().then(res => {
          if (res && res.alterouLocal && typeof onUpdateCallback === 'function') {
            onUpdateCallback();
          }
        });
      }
    });
  }
};
