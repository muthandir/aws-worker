const AWS = require('aws-sdk');

const validate = (config) => {
  if (!config) throw new Error("[AWS-PUBLISHER] Config must be presented");
  if (!config.hasOwnProperty('Region')) throw new Error('[AWS-PUBLISHER] Region must be presented');
};

const SQSService = (config) => {
  let sqs = new AWS.SQS();
  const _sqsService = {};

  _sqsService.send = ({ message, queueUrl = config.QueueUrl, options = {} }) => {
    return new Promise((res, rej) => {
      var params = {
        MessageBody: JSON.stringify({
          Message: JSON.stringify({
            message: message,
            topic: message.topic
          })
        }),
        QueueUrl: queueUrl,
        MessageAttributes: options.MessageAttributes,
        DelaySeconds: options.DelaySeconds,
      };

      const _queueUrl = queueUrl.split('.');
      if (_queueUrl[_queueUrl.length - 1] === 'fifo') {
        params.MessageGroupId = options.MessageGroupId;
        params.MessageDeduplicationId = options.MessageDeduplicationId.toString();
      }

      sqs.sendMessage(params, function (err, result) {
        if (err) {
          return rej(err);
        }

        return res(result);
      });
    });
  };

  _sqsService.get = ({ queueUrl = config.QueueUrl, visibilityTimeout = config.VisibilityTimeout } = {}) => {
    return new Promise((res, rej) => {
      let params = {
        QueueUrl: queueUrl,
        /* required */
        AttributeNames: ['All'],
        VisibilityTimeout: visibilityTimeout
      };

      sqs.receiveMessage(params, function (err, result) {
        if (err) {
          return rej(err);
        }

        return res(result);
      });
    });
  };

  _sqsService.delete = ({ receiptHandle, queueUrl = config.QueueUrl }) => {
    return new Promise((res, rej) => {
      let params = {
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle
      };

      sqs.deleteMessage(params, function (err, result) {
        if (err) {
          return rej(err);
        }

        return res(result);
      });
    });
  };


  return _sqsService;
};


const SNSService = (config) => {
  const _snsService = {};
  let sns = new AWS.SNS();

  _snsService.send = ({ message, topic, topicArn = config.ArnPath }) => {
    return new Promise((res, rej) => {
      let params = {
        Message: JSON.stringify({
          message: message,
          topic: topic
        }),
        MessageStructure: 'sqs',
        TopicArn: topicArn
      };

      sns.publish(params, (err, result) => {
        if (err) {
          return rej(err);
        }

        return res(result);
      });
    });
  };
  _snsService.addDevice = (deviceId, arn = config.ArnPath) => {

    return new Promise((res, rej) => {
      let params = {
        PlatformApplicationArn: arn,
        Token: deviceId,
      };

      sns.createPlatformEndpoint(params, (err, result) => {
        if (err) {
          return rej(err);
        }
        return res(result);
      });
    });
  };
  _snsService.pushNotification = (targetArn, payload) => {
    return new Promise((res, rej) => {
      let params = {
        Message: payload,
        MessageStructure: 'json',
        TargetArn: targetArn,
      };

      sns.publish(params, (err, result) => {
        if (err) {
          return rej(err);
        }
        return res(result);
      });
    });
  };

  _snsService.sendSms = (data) => {
    return new Promise((res, rej) => {
      let DefaultSMSType = 'Promotional';
      const params = {
        Message: data.message,
        PhoneNumber: data.phoneNumber
      };
      if (data.critical) {
        DefaultSMSType = 'Transactional';
      }
      sns.setSMSAttributes({
        attributes: {
          DefaultSMSType,
          DefaultSenderID: data.defaultSenderID // Setliyorum ama turkiye icin suan destegi yok
        }
      });

      sns.publish(params, (err, result) => {
        if (err) {
          return rej(err);
        }
        return res(result);
      });
    });
  };

  return _snsService;
};

/**
 * @param {object} config Configuration object.
 * @param {string} config.Region AWS region.
 * @param {string} config.ArnPath Amazon Resource Name of the SES service.
 * @param {string} config.DefaultSender Email source.
 * @param {'development' | 'production'} config.env Worker environment.
 * @param {string[]} [config.devEmailAddresses] Email addresses to send email messages to in case `env === 'development'`.
 */
const SESService = (config) => {
  AWS.config.region = config.Region;
  const _sesService = {};
  let ses = new AWS.SES();

  if (!config.env) {
    throw new Error('Environment not set');
  }
  if (config.env === 'development' && !Array.isArray(config.devEmailAddresses)) {
    throw new Error('devEmailAddresses is not an array');
  }

  const arrayifyAddresses = (addresses) => (
    Array.isArray(addresses) ? addresses : [addresses]
  ).filter(Boolean); // Filters null and empty strings

  const mapOutboundAddress = (addresses) => {
    switch (config.env) {
      case 'development':
        return config.devEmailAddresses;
      case 'production':
        return addresses;
      default:
        throw new Error('Invalid environment');
    }
  }

  const mapSubject = (subject, toAddresses, ccAddresses) => {
    switch (config.env) {
      case 'development':
        // To addresses always exist
        let devSubject = `[To: ${toAddresses}]`;
        // Cc addresses may or may not exist
        if (ccAddresses && ccAddresses.length) {
          devSubject += ` [Cc: ${ccAddresses}]`
        }
        devSubject += ` ${subject}`;
        return devSubject;
      case 'production':
        return subject;
      default:
        throw new Error('Invalid environment');
    }
  }

  _sesService.send = ({ data, defaultSender = config.DefaultSender, topicArn = config.ArnPath }) => {
    return new Promise((res, rej) => {
      const toAddresses = arrayifyAddresses(data.ToAddresses);
      const ccAddresses = arrayifyAddresses(data.ToCcAddresses);

      if (!toAddresses.length) {
        rej(new Error('ToAddresses is empty'));
        return;
      }
      let params = {
        Destination: {
          BccAddresses: mapOutboundAddress(toAddresses),
          CcAddresses: mapOutboundAddress(ccAddresses),
        },
        Message: {
          Body: {
            Html: {
              Data: data.Body,
              Charset: 'UTF-8'
            },
          },
          Subject: {
            Data: mapSubject(data.Subject, toAddresses, ccAddresses),
            Charset: 'UTF-8'
          }
        },
        Source: defaultSender,
        // ReplyToAddresses: [
        //     config.DefaultSender
        // ],
        ReturnPath: defaultSender,
        ReturnPathArn: topicArn,
        SourceArn: topicArn,
        Tags: data.Tags || []
      };

      ses.sendEmail(params, (err, result) => {
        if (err) {
          return rej(err);
        }

        return res(result);
      });
    });
  };

  return _sesService;
};

const SERVICES = {
  'SQS': SQSService,
  'SNS': SNSService,
  'SES': SESService
};

module.exports = {
  getService: (serviceName, config) => {
    validate(config);
    AWS.config.region = config.Region;

    if (!SERVICES[serviceName]) {
      throw new Error('Invalid service');
    }

    return SERVICES[serviceName](config);
  }
};