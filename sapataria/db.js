// ==================================================
// IndexedDB REPOSITORY & REALTIME SYNC ENGINE
// Sapataria Calce Couro - Atualização Instantânea sem F5
// Suporta: SSE (Tempo Real Local), BroadcastChannel (Abas) e Nuvem Google Sheets
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

      if (!db.objectStoreNames.contains('ordens_servico')) {
        const osStore = db.createObjectStore('ordens_servico', { keyPath: 'id', autoIncrement: true });
        osStore.createIndex('numero_os', 'numero_os', { unique: true });
        osStore.createIndex('status', 'status', { unique: false });
        osStore.createIndex('cliente_nome', 'cliente_nome', { unique: false });
        osStore.createIndex('data_entrada', 'data_entrada', { unique: false });
        osStore.createIndex('sincronizado', 'sincronizado', { unique: false });
      }

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

// Salvar / Atualizar OS Localmente
async function salvarOS(os, dispararSync = true) {
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

        if (dispararSync) {
          // Avisa outras abas abertas no mesmo navegador instantaneamente
          RealtimeSync.broadcastLocal('order_saved', os);

          // Envia para o servidor local (SSE) e para a nuvem
          RealtimeSync.pushOrder(os).catch(err => console.warn('Sync offline em fila:', err));
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

// Obter OS por Número Sequencial
async function obterOSPorNumero(numOS) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction('ordens_servico', 'readonly');
    const store = tx.objectStore('ordens_servico');
    const index = store.index('numero_os');
    const request = index.get(Number(numOS));

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
        RealtimeSync.broadcastLocal('order_deleted', { numero_os: os.numero_os });
        RealtimeSync.deleteOrder(os.numero_os).catch(console.warn);
      }
    };
    request.onerror = () => reject(request.error);
  });
}

// Exportar Backup Completo JSON
async function exportarBackupJSON() {
  const ordens = await listarTodasOS();
  const backup = {
    versao: '2.5',
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
          store.put(item);
          importadas++;
        }

        tx.oncomplete = async () => {
          RealtimeSync.syncAll().catch(console.error);
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
// 2. REAL-TIME SYNCHRONIZATION ENGINE (ZERO F5)
// Três camadas simultâneas:
// - Camada 1: SSE (Server-Sent Events) direto com o servidor Node local
// - Camada 2: BroadcastChannel para comunicação instantânea entre abas
// - Camada 3: Cloud Polling rápido (Google Apps Script) para celulares remotos
// ==================================================
const RealtimeSync = {
  status: 'idle', // 'idle' | 'syncing' | 'synced' | 'offline'
  listeners: [],
  changeCallbacks: [],
  eventSource: null,
  broadcastChannel: null,
  pollTimer: null,
  lastRevisionHash: '',
  hasLocalServer: false,

  onStatusChange(fn) {
    if (typeof fn === 'function') this.listeners.push(fn);
  },

  onDataChange(fn) {
    if (typeof fn === 'function') this.changeCallbacks.push(fn);
  },

  notifyStatus(st, extra = null) {
    this.status = st;
    this.listeners.forEach(fn => {
      try { fn(st, extra); } catch(e) {}
    });
  },

  triggerDataChange(reason, data) {
    this.changeCallbacks.forEach(fn => {
      try { fn(reason, data); } catch(e) {}
    });
  },

  // Inicializa canais de tempo real
  init() {
    this.initBroadcastChannel();
    this.initSSE();
    this.startFastCloudPolling();
  },

  // 1. Canal entre Abas no mesmo navegador
  initBroadcastChannel() {
    try {
      if (typeof BroadcastChannel !== 'undefined') {
        this.broadcastChannel = new BroadcastChannel('calce_couro_realtime_channel');
        this.broadcastChannel.onmessage = async (e) => {
          const { event, data } = e.data || {};
          if (event === 'order_saved' || event === 'status_changed' || event === 'order_deleted') {
            await this.applyIncomingUpdate(data, false);
            this.triggerDataChange(event, data);
          }
        };
      }
    } catch (e) {
      console.warn('BroadcastChannel não suportado neste navegador:', e);
    }
  },

  broadcastLocal(event, data) {
    if (this.broadcastChannel) {
      try {
        this.broadcastChannel.postMessage({ event, data });
      } catch (e) {}
    }
  },

  // 2. Server-Sent Events (SSE) para conexões locais no servidor Node
  initSSE() {
    // Só tenta SSE se estiver rodando via HTTP/HTTPS e não for file://
    if (!window.location.protocol.startsWith('http')) return;

    try {
      if (this.eventSource) this.eventSource.close();
      this.eventSource = new EventSource('/api/sapataria/events');

      this.eventSource.addEventListener('connected', () => {
        this.hasLocalServer = true;
        this.notifyStatus('synced', { mode: 'sse_online' });
      });

      this.eventSource.addEventListener('order_saved', async (e) => {
        try {
          const os = JSON.parse(e.data);
          await this.applyIncomingUpdate(os, false);
          this.notifyStatus('synced', { mode: 'sse' });
          this.triggerDataChange('order_saved', os);
        } catch(err) {
          console.error('Erro ao processar order_saved SSE:', err);
        }
      });

      this.eventSource.addEventListener('status_changed', async (e) => {
        try {
          const os = JSON.parse(e.data);
          await this.applyIncomingUpdate(os, false);
          this.notifyStatus('synced', { mode: 'sse' });
          this.triggerDataChange('status_changed', os);
        } catch(err) {
          console.error('Erro ao processar status_changed SSE:', err);
        }
      });

      this.eventSource.addEventListener('order_deleted', async (e) => {
        try {
          const payload = JSON.parse(e.data);
          const existing = await obterOSPorNumero(payload.numero_os);
          if (existing) {
            const db = await openDB();
            const tx = db.transaction('ordens_servico', 'readwrite');
            tx.objectStore('ordens_servico').delete(existing.id);
          }
          this.triggerDataChange('order_deleted', payload);
        } catch(err) {}
      });

      this.eventSource.onerror = () => {
        // Se desconectar, tenta reconectar e recorre ao polling
        this.hasLocalServer = false;
        this.notifyStatus('offline');
      };
    } catch(err) {
      this.hasLocalServer = false;
    }
  },

  // Aplica ordem vinda de outro aparelho diretamente no IndexedDB
  async applyIncomingUpdate(os, dispararBroadcast = false) {
    const existing = await obterOSPorNumero(os.numero_os);
    const db = await openDB();
    const tx = db.transaction('ordens_servico', 'readwrite');
    const store = tx.objectStore('ordens_servico');

    if (existing) {
      os.id = existing.id;
      store.put(os);
    } else {
      delete os.id;
      store.add(os);
    }

    await new Promise((res, rej) => {
      tx.oncomplete = res;
      tx.onerror = rej;
    });

    if (dispararBroadcast) {
      this.broadcastLocal('order_saved', os);
    }
  },

  // 3. Enviar Ordem para o Servidor Local & Nuvem
  async pushOrder(os) {
    const autor = localStorage.getItem('calce_operador_nome') || 'Oficina (Pais)';
    os.sincronizado_por = autor;

    this.notifyStatus('syncing');

    // 1. Tenta API local Node (sub-segundo)
    let enviouLocal = false;
    if (window.location.protocol.startsWith('http')) {
      try {
        const res = await fetch('/api/sapataria/ordens', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(os)
        });
        if (res.ok) {
          enviouLocal = true;
          this.notifyStatus('synced');
        }
      } catch (e) {}
    }

    // 2. Envia também para o Google Sheets (Apps Script) em segundo plano
    this.pushGoogleSheets(os, autor).catch(err => {
      console.warn('Google Sheets sync em fila:', err);
    });

    if (!enviouLocal) {
      this.notifyStatus('offline');
    }
    return true;
  },

  // Atualizar Status Rápido no Servidor & Nuvem
  async updateStatus(numero_os, novoStatus) {
    const autor = localStorage.getItem('calce_operador_nome') || 'Oficina (Pais)';

    // Atualiza localmente no IndexedDB
    const existing = await obterOSPorNumero(numero_os);
    if (existing) {
      existing.status = novoStatus;
      existing.atualizado_em = new Date().toISOString();
      await salvarOS(existing, false);
      this.broadcastLocal('status_changed', existing);
      this.triggerDataChange('status_changed', existing);
    }

    // Envia ao servidor Node
    if (window.location.protocol.startsWith('http')) {
      fetch(`/api/sapataria/ordens/${numero_os}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: novoStatus, autor })
      }).catch(console.warn);
    }

    // Envia ao Google Sheets
    const code = `
    (function() {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheetByName('Sapataria_Ordens');
      if (!sheet) return { error: 'Aba nao encontrada' };
      var data = sheet.getDataRange().getValues();
      var numOS = Number(${Number(numero_os)});
      for (var i = 1; i < data.length; i++) {
        if (Number(data[i][0]) === numOS) {
          sheet.getRange(i + 1, 10).setValue(${JSON.stringify(novoStatus)});
          sheet.getRange(i + 1, 13).setValue(new Date().toISOString());
          sheet.getRange(i + 1, 14).setValue(${JSON.stringify(autor)});
          return { success: true };
        }
      }
      return { error: 'OS nao encontrada' };
    })()
    `;
    fetch(APPS_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'execute_command', code })
    }).catch(console.warn);
  },

  // Deletar Ordem no Servidor & Nuvem
  async deleteOrder(numero_os) {
    if (window.location.protocol.startsWith('http')) {
      fetch(`/api/sapataria/ordens/${numero_os}`, {
        method: 'DELETE'
      }).catch(console.warn);
    }

    const code = `
    (function() {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheetByName('Sapataria_Ordens');
      if (!sheet) return;
      var data = sheet.getDataRange().getValues();
      var numOS = Number(${Number(numero_os)});
      for (var i = 1; i < data.length; i++) {
        if (Number(data[i][0]) === numOS) {
          sheet.deleteRow(i + 1);
          return { deleted: true };
        }
      }
    })()
    `;
    fetch(APPS_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'execute_command', code })
    }).catch(console.warn);
  },

  // Salva no Google Sheets
  async pushGoogleSheets(os, autor) {
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
      return { success: true, numero_os: numOS };
    })()
    `;

    return fetch(APPS_SCRIPT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'execute_command', code })
    });
  },

  // 4. Polling Rápido Inteligente (Zero F5 em qualquer rede)
  startFastCloudPolling() {
    if (this.pollTimer) clearInterval(this.pollTimer);

    // Roda sync completo inicial
    this.syncAll();

    // Roda a cada 4 segundos
    this.pollTimer = setInterval(async () => {
      // Se tiver SSE ativo e funcionando no servidor local, o SSE já faz o trabalho em tempo real!
      // Mas se não tiver SSE ou estiver em aba de fundo, o polling garante 100% de consistência
      if (document.hidden) return;
      await this.checkRemoteUpdates();
    }, 4000);

    // Quando o usuário toca na tela ou volta à aba, checa imediatamente
    window.addEventListener('visibilitychange', () => {
      if (!document.hidden) this.checkRemoteUpdates();
    });
    window.addEventListener('focus', () => this.checkRemoteUpdates());
  },

  // Verifica se há novas atualizações remotas de forma super leve
  async checkRemoteUpdates() {
    // 1. Se estiver conectado ao Node server local, checa API local primeiro (0.01s)
    if (window.location.protocol.startsWith('http')) {
      try {
        const res = await fetch('/api/sapataria/ordens');
        if (res.ok) {
          const json = await res.json();
          if (json.success && Array.isArray(json.data)) {
            const newHash = json.data.map(o => `${o.numero_os}:${o.status}:${o.atualizado_em}`).join('|');
            if (newHash !== this.lastRevisionHash) {
              this.lastRevisionHash = newHash;
              // Salva todas no IndexedDB
              const db = await openDB();
              const tx = db.transaction('ordens_servico', 'readwrite');
              const store = tx.objectStore('ordens_servico');
              json.data.forEach(item => {
                store.put(item);
              });
              await new Promise(r => tx.oncomplete = r);
              this.notifyStatus('synced');
              this.triggerDataChange('poll_update', json.data);
              return;
            }
          }
        }
      } catch (e) {}
    }

    // 2. Se não houver servidor local (ex: no GitHub Pages), checa Google Sheets
    try {
      const code = `
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
          list.push({
            numero_os: Number(row[0]) || i,
            data_entrada: String(row[1] || ''),
            cliente_nome: String(row[2] || ''),
            cliente_whatsapp: String(row[3] || ''),
            itens: itens,
            previsao_retirada_data: prevParts[0] || '',
            previsao_retirada_hora: prevParts[1] || '',
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

      const res = await fetch(APPS_SCRIPT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'execute_command', code })
      });
      const data = await res.json();
      if (data.success && Array.isArray(data.result)) {
        const newHash = data.result.map(o => `${o.numero_os}:${o.status}:${o.atualizado_em}`).join('|');
        if (newHash !== this.lastRevisionHash) {
          this.lastRevisionHash = newHash;
          const db = await openDB();
          const tx = db.transaction('ordens_servico', 'readwrite');
          const store = tx.objectStore('ordens_servico');
          data.result.forEach(item => {
            store.put(item);
          });
          await new Promise(r => tx.oncomplete = r);
          this.notifyStatus('synced');
          this.triggerDataChange('poll_update', data.result);
        }
      }
    } catch (e) {
      this.notifyStatus('offline');
    }
  },

  // Sincronização manual imediata
  async syncAll() {
    this.notifyStatus('syncing');
    await this.checkRemoteUpdates();
    this.notifyStatus('synced');
  }
};

// Compatibilidade de nome com referências existentes
const CloudSync = {
  sync: () => RealtimeSync.syncAll(),
  pushOrder: (os) => RealtimeSync.pushOrder(os),
  updateStatus: (num, st) => RealtimeSync.updateStatus(num, st),
  deleteOrder: (num) => RealtimeSync.deleteOrder(num),
  onStatusChange: (fn) => RealtimeSync.onStatusChange(fn),
  startAutoSync: () => {}
};
