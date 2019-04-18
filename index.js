const azure = require ('azure-storage');
const fs = require ('fs');
const path = require ('path');

require ('dotenv').config ();

const {QUEUE_CONNECTION_STRING, QUEUE_NAME, OUT_PATH} = process.env;

var queueSvc = azure.createQueueService (QUEUE_CONNECTION_STRING);

function processMessages () {
  return new Promise (function (resolve, reject) {
    queueSvc.getMessages (QUEUE_NAME, function (error, messages, response) {
      const messageCount = messages.length;
      if (error) reject (error);

      // Message text is in results[0].messageText
      for (let message of messages) {
        const body = JSON.parse (
          Buffer.from (message.messageText, 'base64').toString ('utf8')
        );

        const baseFilename =
          body.eventTime.replace (/\:/g, '-') +
          '-' +
          body.eventType +
          '-' +
          body.id;

        const filename = path.join (OUT_PATH, baseFilename + '.json');
        console.log ('Message:', message.messageId, filename);
        fs.writeFileSync (filename, JSON.stringify (body, null, ' '), 'utf8');

        queueSvc.deleteMessage (
          queueName,
          message.messageId,
          message.popReceipt,
          function (error) {
            if (error) {
              reject (error);
            }
          }
        );
      }
      resolve (messageCount);
    });
  });
}

async function main () {
  let procCount;

  while (true) {
    procCount = await processMessages ();
    if (procCount === 0) break;
  }
}

main ().then (() => {
  console.log ('done');
});
