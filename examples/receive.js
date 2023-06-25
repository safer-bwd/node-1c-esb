const { ReceiverEvents, delay } = require('rhea-promise');
const { Connection, ConnectionEvents } = require('../src');

const url = 'http://localhost:9090/applications/portal-trade';
const clientKey = '';
const clientSecret = '';
const process = 'Основной::ВыгрузкаЗаказов';
const channel = 'to_trade';

// eslint-disable-next-line no-console
const log = (...args) => console.log(`${JSON.stringify(new Date())}`, ...args);

const connection = new Connection({
  url, clientKey, clientSecret, reconnect: false,
});

connection.on(ConnectionEvents.connectionOpen, () => log('Connection opened'));
connection.on(ConnectionEvents.connectionClose, () => log('Connection closed'));
connection.on(ConnectionEvents.connectionError, (ctx) => log('Connection error', ctx.error));
connection.on(ConnectionEvents.disconnected, (ctx) => log('Connection error', ctx.error));

(async () => {
  await connection.open();

  const receiver = await connection.createReceiver(process, channel);
  receiver.on(ReceiverEvents.message, (ctx) => {
    log('Received message ->', ctx.message);
  });

  await delay(5 * 1000);
  await receiver.close();
  await connection.close();
})().catch((err) => log('Send error:', err));
