const AWS = require('aws-sdk');
const util = require('util');
const s3 = new AWS.S3();
const { Pool } = require('pg');
const fetch = require('node-fetch');
const parsePgConnectionString = require('pg-connection-string');

const herokuApiKey = process.env.HEROKU_API_KEY;
const herokuPostgresId = process.env.HEROKU_POSTGRES_ID;
let dbPool;

async function initializePgPool() {
  const herokuConfig = await fetch(`https://api.heroku.com/addons/${herokuPostgresId}/config`, {
    headers: {
      'Authorization': `Bearer ${herokuApiKey}`,
      'Accept': 'application/vnd.heroku+json; version=3'
    }
  })
    .then(res => res.json())
    .then(data => data)
    .catch((err) => err)

  if (herokuConfig instanceof Error) {
    return
  }

  const pgConfig = {
    ...parsePgConnectionString(herokuConfig[0].value), // the db string returned by heroku
    max: 1,
    ssl: {
      rejectUnauthorized: false
    },
    idleTimeoutMillis: 120000,
    connectionTimeoutMillis: 10000,
  }
  dbPool = new Pool(pgConfig)
}

async function deleteDirectory(dir) {
    const bucket = process.env.RESIZED_PHOTOS_BUCKET;

    const listParams = {
        Bucket: bucket,
        Prefix: dir
    }

    try {
        const listedObjects = await s3.listObjectsV2(listParams).promise();

        if (listedObjects.Contents.length === 0) return;
    
        const deleteParams = {
            Bucket: bucket,
            Delete: { Objects: listedObjects.Contents.map(({Key }) => Key) }
        };
    
        await s3.deleteObjects(deleteParams).promise();
    
        // keep going if more keys exist
        if (listedObjects.IsTruncated) await deleteDirectory(dir);
        return Promise.resolve('finished')
    } catch (err) {
        return Promise.reject('failed')
    }
}

const outputPhotoPrefixes = ['_small', '_small@2x', '_large', '_large@2x'];
const outputFormats = ['avif', 'webp', 'jpeg'];

/* This is only run for delete events */
exports.handler = async (event, context) => {
  console.log('Reading options from event:\n', util.inspect(event, { depth: 5 }));
  const record = event.Records[0];
  const srcBucket = record.s3.bucket.name;
  // Object key may have spaces or unicode non-ASCII characters.
  const srcKey    = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "))

  // Dont wait for the db dbPool connection to close
  context.callbackWaitsForEmptyEventLoop = false;

  // Make sure the object being deleted is a file, if not, try to delete a directory
  const typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.log('The file from event does not have an extension. May be a directory.');
    // so delete all keys with this prefix
    await deleteDirectory(srcKey);
    return;
  }

  const fileName = srcKey.match(/[^/]+$/g);
  const fileNameWithoutExt = fileName[0].replace(typeMatch[0], '');
  const dirWithoutFile = srcKey.replace(fileName, '');
  console.table([fileName, fileNameWithoutExt, dirWithoutFile]);

  // Delete any images from photos-resized that exist
  // There is no way to delete images with a prefix, so the same
  // prefixes that were used to create objects are stored here as well

  console.time(`deleting resized photos of ${fileNameWithoutExt}`);
  await s3
    .deleteObjects({
      Bucket: process.env.RESIZED_PHOTOS_BUCKET,
      Delete: {
        Objects: outputPhotoPrefixes
          .map((prefix) =>
            outputFormats.map((format) => ({
              Key: `${dirWithoutFile}${fileNameWithoutExt}${prefix}.${format}`,
            }))
          )
          .flat(1),
      },
    })
    .promise()
    .then((data) => console.log('successfully deleted photos ', data))
    .catch((err) => {
      console.error(err);
      return Promise.reject('could not delete photos');
    })
    .finally(() => console.timeEnd(`deleting resized photos of ${fileNameWithoutExt}`));

  const query = {
    text: 'DELETE FROM photos_meta WHERE name=$1 and dir_path=$2',
    values: [fileNameWithoutExt, dirWithoutFile],
  };
  

  if (!dbPool) {
    console.log('initializing dbPool')
    console.time('get heroku db url and initialize dbPool')
    await initializePgPool();
    console.timeEnd('get heroku db url and initialize dbPool')
    if (!dbPool) {
      return Promise.reject('could not initalize db dbPool')
    }
  } else {
    console.log('dbPool already initialized')
  }

  console.time('delete from db');
  await dbPool
    .query(query)
    .then((res) => console.log(res))
    .catch((err) => {
      console.error(err);
      return Promise.reject('failed to delete from db');
    })
    .finally(() => console.timeEnd('delete from db'));
  return Promise.resolve(
    `delete image ${fileNameWithoutExt} from resized bucket and from photos meta db`
  );
};