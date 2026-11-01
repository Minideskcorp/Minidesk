/**
 * =========================================================================
 * MINIDESK FIREBASE REALTIME EXTENSION (Live Sync Engine)
 * =========================================================================
 * 
 * Provides instantaneous sub-50ms multi-device synchronization for Minidesk.
 * Operates purely as a non-invasive layer on top of index.html.
 * All existing code, Google Sheet storage, and UI transitions are 100% preserved.
 */

(function() {
  'use strict';

  // 1. Firebase Configuration
  var FIREBASE_CONFIG = {
    apiKey: "AIzaSyBLQmiEw_UG0k52M3jzGsAsUzfmReydwUk",
    authDomain: "friends-lounge-2b9d8.firebaseapp.com",
    databaseURL: "https://friends-lounge-2b9d8-default-rtdb.asia-southeast1.firebasedatabase.app",
    projectId: "friends-lounge-2b9d8",
    storageBucket: "friends-lounge-2b9d8.firebasestorage.app",
    messagingSenderId: "795360264693",
    appId: "1:795360264693:web:d9ffb9d607740c8ef1e0bc",
    measurementId: "G-PNB9LVSMWL"
  };

  // Unique client session ID to prevent duplicate echoes of local actions
  var MY_CLIENT_ID = 'client_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);

  var firebaseDb = null;
  var isFirebaseReady = false;
  var initialFirebaseLoadDone = false;

  var tasksMap = new Map();
  var memosMap = new Map();
  var subjectsMap = new Map();

  var pendingFastResponse = null;
  var retryTimer = null;

  // Initialize Firebase Realtime Database
  function initFirebase() {
    if (typeof firebase === 'undefined') {
      console.warn('[Firebase Minidesk] Firebase SDK not detected.');
      return false;
    }
    try {
      if (!firebase.apps.length) {
        firebase.initializeApp(FIREBASE_CONFIG);
      }
      firebaseDb = firebase.database();
      isFirebaseReady = true;
      console.log('⚡ [Firebase Minidesk] Connected to Realtime Database:', FIREBASE_CONFIG.databaseURL);
      setupFirebaseListeners();
      return true;
    } catch (err) {
      console.warn('[Firebase Minidesk] Init error:', err);
      return false;
    }
  }

  // Setup Real-time Database Listeners
  function setupFirebaseListeners() {
    if (!firebaseDb) return;

    var tasksRef = firebaseDb.ref('minidesk/tasks');
    var memosRef = firebaseDb.ref('minidesk/memos');
    var subjectsRef = firebaseDb.ref('minidesk/subjects');

    // 1. Load initial dataset from Firebase
    Promise.all([
      tasksRef.once('value'),
      memosRef.once('value'),
      subjectsRef.once('value')
    ]).then(function(snapshots) {
      var tasksSnap = snapshots[0].val();
      var memosSnap = snapshots[1].val();
      var subjectsSnap = snapshots[2].val();

      if (tasksSnap) {
        Object.keys(tasksSnap).forEach(function(k) {
          if (tasksSnap[k] && tasksSnap[k].id) tasksMap.set(String(tasksSnap[k].id), tasksSnap[k]);
        });
      }
      if (memosSnap) {
        Object.keys(memosSnap).forEach(function(k) {
          if (memosSnap[k] && memosSnap[k].id) memosMap.set(String(memosSnap[k].id), memosSnap[k]);
        });
      }
      if (subjectsSnap) {
        Object.keys(subjectsSnap).forEach(function(k) {
          if (subjectsSnap[k] && subjectsSnap[k].id) subjectsMap.set(String(subjectsSnap[k].id), subjectsSnap[k]);
        });
      }

      initialFirebaseLoadDone = true;

      // If Firebase already has items, deliver them immediately to Minidesk!
      if (tasksMap.size > 0 || memosMap.size > 0 || subjectsMap.size > 0) {
        triggerFastSyncToUI();
      }
    }).catch(function(err) {
      console.warn('[Firebase Minidesk] Initial snapshot error:', err);
      initialFirebaseLoadDone = true;
    });

    // 2. Real-time Task Listeners
    tasksRef.on('child_added', function(snapshot) {
      if (!initialFirebaseLoadDone) return;
      var val = snapshot.val();
      if (!val || !val.id) return;
      if (val._client_id === MY_CLIENT_ID) return; // Skip own action
      tasksMap.set(String(val.id), val);
      triggerFastSyncToUI();
    });

    tasksRef.on('child_changed', function(snapshot) {
      var val = snapshot.val();
      if (!val || !val.id) return;
      if (val._client_id === MY_CLIENT_ID) return; // Skip own action
      tasksMap.set(String(val.id), val);
      triggerFastSyncToUI();
    });

    tasksRef.on('child_removed', function(snapshot) {
      var key = snapshot.key;
      var val = snapshot.val();
      var id = (val && val.id) ? String(val.id) : String(key);
      tasksMap.delete(id);
      triggerFastSyncToUI();
    });

    // 3. Real-time Memo Listeners
    memosRef.on('child_added', function(snapshot) {
      if (!initialFirebaseLoadDone) return;
      var val = snapshot.val();
      if (!val || !val.id) return;
      if (val._client_id === MY_CLIENT_ID) return;
      memosMap.set(String(val.id), val);
      triggerFastSyncToUI();
    });

    memosRef.on('child_changed', function(snapshot) {
      var val = snapshot.val();
      if (!val || !val.id) return;
      if (val._client_id === MY_CLIENT_ID) return;
      memosMap.set(String(val.id), val);
      triggerFastSyncToUI();
    });

    memosRef.on('child_removed', function(snapshot) {
      var key = snapshot.key;
      var val = snapshot.val();
      var id = (val && val.id) ? String(val.id) : String(key);
      memosMap.delete(id);
      triggerFastSyncToUI();
    });

    // 4. Real-time Subject Listeners
    subjectsRef.on('child_added', function(snapshot) {
      if (!initialFirebaseLoadDone) return;
      var val = snapshot.val();
      if (!val || !val.id) return;
      if (val._client_id === MY_CLIENT_ID) return;
      subjectsMap.set(String(val.id), val);
      triggerFastSyncToUI();
    });

    subjectsRef.on('child_removed', function(snapshot) {
      var key = snapshot.key;
      var val = snapshot.val();
      var id = (val && val.id) ? String(val.id) : String(key);
      subjectsMap.delete(id);
      triggerFastSyncToUI();
    });
  }

  // Fast trigger to invoke Minidesk's native reconciliation engine (silentSync)
  function triggerFastSyncToUI() {
    pendingFastResponse = {
      status: "success",
      server_version: Date.now(),
      tasks: Array.from(tasksMap.values()),
      memos: Array.from(memosMap.values()),
      subjects: Array.from(subjectsMap.values())
    };

    if (typeof window.silentSync === 'function') {
      window.silentSync();
    }

    // Backup retries in case silentSync was currently busy
    clearTimeout(retryTimer);
    retryTimer = setTimeout(function() {
      if (pendingFastResponse && typeof window.silentSync === 'function') {
        window.silentSync();
      }
    }, 120);
  }

  // Hook window.fetch so silentSync instantly gets Firebase data when ready
  var origFetch = window.fetch;
  window.fetch = function(url, options) {
    if (typeof url === 'string' && url.includes('script.google.com') && (!options || options.method === 'GET' || !options.method)) {
      if (pendingFastResponse) {
        var fastData = pendingFastResponse;
        pendingFastResponse = null;
        return Promise.resolve(new Response(JSON.stringify(fastData), {
          status: 200,
          headers: { 'Content-Type': 'application/json' }
        }));
      }
    }

    var fetchPromise = origFetch.apply(this, arguments);

    // When a real Google Sheet fetch completes: seed Firebase if Firebase was empty
    if (typeof url === 'string' && url.includes('script.google.com') && (!options || options.method === 'GET' || !options.method)) {
      fetchPromise.then(function(res) {
        if (!res.ok) return;
        res.clone().json().then(function(sheetData) {
          if (sheetData && sheetData.status === "success" && isFirebaseReady && firebaseDb) {
            seedFirebaseFromSheet(sheetData);
          }
        }).catch(function() {});
      }).catch(function() {});
    }

    return fetchPromise;
  };

  // Seed Firebase with Google Sheet data if Firebase has no records
  function seedFirebaseFromSheet(sheetData) {
    if (!firebaseDb) return;
    if (tasksMap.size === 0 && Array.isArray(sheetData.tasks) && sheetData.tasks.length > 0) {
      sheetData.tasks.forEach(function(t) {
        if (t && t.id) {
          tasksMap.set(String(t.id), t);
          firebaseDb.ref('minidesk/tasks/' + t.id).set(t);
        }
      });
    }
    if (memosMap.size === 0 && Array.isArray(sheetData.memos) && sheetData.memos.length > 0) {
      sheetData.memos.forEach(function(m) {
        if (m && m.id) {
          memosMap.set(String(m.id), m);
          firebaseDb.ref('minidesk/memos/' + m.id).set(m);
        }
      });
    }
    if (subjectsMap.size === 0 && Array.isArray(sheetData.subjects) && sheetData.subjects.length > 0) {
      sheetData.subjects.forEach(function(s) {
        if (s && s.id) {
          subjectsMap.set(String(s.id), s);
          firebaseDb.ref('minidesk/subjects/' + s.id).set(s);
        }
      });
    }
  }

  // Hook window.syncToSheet to instantly mirror user actions to Firebase
  function installSyncToSheetHook() {
    var checkInterval = setInterval(function() {
      if (typeof window.syncToSheet === 'function') {
        clearInterval(checkInterval);

        var origSyncToSheet = window.syncToSheet;
        window.syncToSheet = function(payload) {
          // 1. Execute original syncToSheet (Google Sheets permanent backup)
          try {
            origSyncToSheet.apply(this, arguments);
          } catch (e) {
            console.warn('[Minidesk] Original syncToSheet error:', e);
          }

          // 2. Instantly push to Firebase Realtime Database in <50ms
          try {
            mirrorActionToFirebase(payload);
          } catch (err) {
            console.warn('[Firebase Minidesk] Mirror action error:', err);
          }
        };

        console.log('⚡ [Firebase Minidesk] Real-time write hook successfully attached to syncToSheet');
      }
    }, 50);
  }

  // Mirror action into Firebase Realtime Database
  function mirrorActionToFirebase(payload) {
    if (!isFirebaseReady || !firebaseDb || !payload) return;

    var tasksRef = firebaseDb.ref('minidesk/tasks');
    var memosRef = firebaseDb.ref('minidesk/memos');
    var subjectsRef = firebaseDb.ref('minidesk/subjects');

    var type = payload.type;
    var action = payload.action;

    if (type === 'task') {
      var taskId = String(payload.id);
      if (action === 'add') {
        var taskData = Object.assign({}, payload, {
          _client_id: MY_CLIENT_ID,
          _updated_at: Date.now()
        });
        tasksMap.set(taskId, taskData);
        tasksRef.child(taskId).set(taskData);
      } else if (action === 'edit') {
        var editData = Object.assign({}, payload, {
          _client_id: MY_CLIENT_ID,
          _updated_at: Date.now()
        });
        tasksMap.set(taskId, editData);
        tasksRef.child(taskId).update(editData);
      } else if (action === 'toggle') {
        var card = document.getElementById('task-card-' + taskId);
        var isDone = card ? card.classList.contains('completed') : false;
        var newStatus = isDone ? 'completed' : 'pending';
        var existing = tasksMap.get(taskId) || {};
        existing.status = newStatus;
        existing._client_id = MY_CLIENT_ID;
        existing._updated_at = Date.now();
        tasksMap.set(taskId, existing);
        tasksRef.child(taskId).update({
          status: newStatus,
          _client_id: MY_CLIENT_ID,
          _updated_at: Date.now()
        });
      } else if (action === 'delete') {
        tasksMap.delete(taskId);
        tasksRef.child(taskId).remove();
      }
    } else if (type === 'memo') {
      var memoId = String(payload.id);
      if (action === 'add' || action === 'edit') {
        var memoData = Object.assign({}, payload, {
          _client_id: MY_CLIENT_ID,
          _updated_at: Date.now()
        });
        memosMap.set(memoId, memoData);
        if (action === 'add') {
          memosRef.child(memoId).set(memoData);
        } else {
          memosRef.child(memoId).update(memoData);
        }
      } else if (action === 'delete') {
        memosMap.delete(memoId);
        memosRef.child(memoId).remove();
      }
    } else if (type === 'subject') {
      var subjId = String(payload.id);
      if (action === 'add') {
        var subjData = {
          id: subjId,
          name: payload.name,
          _client_id: MY_CLIENT_ID,
          _updated_at: Date.now()
        };
        subjectsMap.set(subjId, subjData);
        subjectsRef.child(subjId).set(subjData);
      } else if (action === 'delete') {
        subjectsMap.delete(subjId);
        subjectsRef.child(subjId).remove();
      }
    }
  }

  // Lifecycle Start
  initFirebase();
  installSyncToSheetHook();

})();
