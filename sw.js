/* Service Worker — Viva Las Vegas 2026 Push Notifications */

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim());
});

self.addEventListener('push', (event) => {
  let data = { title: '🤖 Puter', body: 'has something to say...' };
  try {
    if (event.data) {
      data = event.data.json();
    }
  } catch (e) {
    // fallback
  }
  
  const options = {
    body: data.body,
    icon: data.icon || '/puter-icon.png',
    badge: '/puter-badge.png',
    tag: data.tag || 'puter-notification',
    renotify: true,
    data: {
      url: data.url || '/',
      type: data.type || 'taunt'
    },
    actions: data.actions || [],
    vibrate: [200, 100, 200]
  };

  event.waitUntil(
    self.registration.showNotification(data.title || '🤖 Puter', options)
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  
  const url = event.notification.data?.url || '/';
  
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(windowClients => {
      // Focus existing tab if open
      for (const client of windowClients) {
        if (client.url.includes(self.location.origin) && 'focus' in client) {
          return client.focus();
        }
      }
      // Otherwise open new tab
      return clients.openWindow(url);
    })
  );
});
