const { generate_uuid: uuid, message: rheaMessage } = require('rhea-promise');
const { Connection, ConnectionEvents } = require('../src');

const url = 'http://localhost:9090/applications/portal-trade';
const clientKey = '';
const clientSecret = '';
const process = 'Основной::ВыгрузкаЗаказов';
const channel = 'from_portal';

// eslint-disable-next-line no-console
const log = (...args) => console.log(`${JSON.stringify(new Date())}`, ...args);

const createMessage = () => {
  const messageId = uuid();
  const payload = { messageId };
  return {
    message_id: messageId,
    body: rheaMessage.data_section(Buffer.from(JSON.stringify(payload), 'utf8')),
    application_properties: {
      integ_message_id: messageId,
    },
  };
};

const connection = new Connection({
  url, clientKey, clientSecret, amqp: { reconnect: false }
});

connection.on(ConnectionEvents.connectionOpen, () => log('Connection opened'));
connection.on(ConnectionEvents.connectionClose, () => log('Connection closed'));
connection.on(ConnectionEvents.connectionError, (ctx) => log('Connection error', ctx.error));
connection.on(ConnectionEvents.disconnected, (ctx) => log('Connection error', ctx.error));

(async () => {
  await connection.open();
  const message = createMessage();
  const sender = await connection.createAwaitableSender(process, channel);
  await sender.send(message);
  log('Sent message -->', message);
  await sender.close();
  await connection.close();
})().catch((err) => log('Send error:', err));
