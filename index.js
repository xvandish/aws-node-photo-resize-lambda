const AWS = require('aws-sdk');
const sharp = require('sharp');
const util = require('util');
const s3 = new AWS.S3();
const { Pool } = require('pg');
const fetch = require('node-fetch')
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

/**
 *
 * @param {string} bucket - name of the bucket
 * @param {string} key - name of the key
 * @param {AWS.S3} s3 - instantiated s3 object
 */
async function getOriginalImage(bucket, key) {
  const imageLocation = { Bucket: bucket, Key: key };
  console.log('trying to getOriginalImage');
  try {
    const originalImage = await s3.getObject(imageLocation).promise();
    console.log('got back originalImage', originalImage.Body.length);
    return originalImage;
  } catch (err) {
    console.error(err);
    return new Error('failed to download original image');
  }
}

/**
 *
 * @param {AWS.S3.GetObjectOutput} imageBlobFromS3
 * @param {string} imgNameNoExt
 * Attempt to get altText from the metadata map stored by the file in S3
 * Otherwise, use the image name, stripped of separators, as the altText
 */
function getImageAltText(imageBlobFromS3, imgNameNoExt) {
  const { altText } = imageBlobFromS3.Metadata;
  if (altText) return altText;

  // If there is no user defined alt text, take the img name with
  // any separator between words (hyphen,underscore, anything,) stripped out
  return imgNameNoExt.replace(/[^A-Z0-9]/gi, ' ');
}

/**
 *
 * @param {sharp.Sharp} sharpImage
 * @param {string} desiredFormat
 */
function convertImageToFormatAndReturnBuffer(sharpImage, desiredFormat) {
  return sharpImage[desiredFormat.format]({ quality: desiredFormat.quality }).toBuffer();
}

function uploadImageToS3(
  imgBytes,
  imgName,
  imgFormat,
  imgSizePrefix,
  imgDir,
  bucketName
) {
  const key = `${imgDir}${imgName}${imgSizePrefix}.${imgFormat}`;
  console.time(`s3 upload for ${key}`);

  return s3
    .putObject({
      Bucket: bucketName,
      Key: `${imgDir}${imgName}${imgSizePrefix}.${imgFormat}`,
      Body: imgBytes,
      ContentType: `image/${imgFormat}`,
    })
    .promise()
    .finally(() => console.timeEnd(`s3 upload for ${key}`));
}

const outputPhotoSizes = [
  { prefix: '_small', width: 333 },
  { prefix: '_small@2x', width: 667 },
  { prefix: '_large', width: 1500 },
  { prefix: '_large@2x', Width: 3000 },
];

const outputFormats = [
  { format: 'webp', quality: 82 },
  { format: 'jpeg', quality: 80 },
  { format: 'avif', quality: 64 },
];

const supportedFormats = ['jpeg', 'jpg', 'png'];

/**
 *
 * @param {*} event an S3 Event Notification https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
 * 1. Check if notification was for a file
 * 2. Check if it was for an image, and if so, the right format
 * 3. For every size in photoSizes, resize the image.
 *    3.a. Convert each size into jpeg, webp and avif formats
 * 4. Upload the ~12 files to S3
 * 5. Update our RDS db with the photo meta of the new picture
 */
exports.handler = async (event, context) => {
  console.log('Reading options from event:\n', util.inspect(event, { depth: 5 }));
  const record = event.Records[0];
  const srcBucket = record.s3.bucket.name;
  const srcKey = record.s3.object.key;

  // Get the extension from the file name
  const typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.log('The file from event does not have an extension. May be a directory.');
    return;
  }

  // Check that the image type is supported
  const imageType = typeMatch[1].toLowerCase();
  if (!supportedFormats.includes(imageType)) {
    console.log(`Unsupported image type: ${imageType}`);
    return;
  }

  console.table([srcBucket, srcKey, imageType]);

  const originalImage = await getOriginalImage(srcBucket, srcKey);
  if (originalImage instanceof Error) return;
  console.log('succesfully recieved originalImage');

  // To store our results, we'll need a db connection
  // If we can't get one, terminate, as we don't want to store results in S3 with
  // no record of them existing in the DB

  // Dont wait for the db dbPool connection to close
  context.callbackWaitsForEmptyEventLoop = false;

  // later on, I could make the request to the db as
  // this lambda -> queue -> worker that inserts into the db
  // for the moment, no need
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


  let client;
  client = await dbPool
    .connect()
    .then((client) => {
      console.log('recieved client succesfully');
      return client;
    })
    .catch((err) => {
      console.error('could not get client from PG dbPool');
      console.error(err.stack);
      return Promise.reject('could not initialize db call');
    });
  
  if (client instanceof Error) {
    return client;  // return the error
  }

  console.log('succesfully connected to client');
  console.time('create initial sharp instance');
  // Add rotation because even if the EXIF metadata of the final images
  // mentions the correct orientation, it isn't applied automatically
  const startingImage = await sharp(originalImage.Body).rotate();
  console.timeEnd('create initial sharp instance');

  // Derive information about the image based on path
  // TODO: fix this next line in case subAlbum is null
  var countOfSlash = srcKey.match(/\//g).length;
  const hasSubalbum = countOfSlash === 3;
  let year;
  let album;
  let subAlbum = null;
  let fileNameWithExt;
  const pathParts = srcKey.split('/'); // 2020/nyc/manhattan_valley/thing.jpeg -> [2020, nyc, manhattan_valley, thing.jpeg]
  if (hasSubalbum) {
    [year, album, subAlbum, fileNameWithExt] = pathParts;
  } else {
    [year, album, fileNameWithExt] = pathParts;
  }
  const imgName = fileNameWithExt.replace(typeMatch[0], ''); // replace the extension with an empty string
  const imgDir = pathParts.slice(0, -1).join('/') + '/'; // path with trailing slash attatched
  const { width, height } = await startingImage.metadata();
  if (width === undefined || height === undefined) {
    return Promise.reject('could not determine width or height');
  }
  const altText = getImageAltText(originalImage, imgName);

  console.table([year, album, subAlbum, fileNameWithExt, imgDir, imgName]);
  // THis is infelxible in terms of forcing an year/album/subalbum/photo structure, can revisit later to support n# of arbitraty connections

  // This is going to get messy, in terms of if 1 fails, what do I do?
  // a worker queue would be a simple way of doing this, let me look into options
  // if that fails, then I can set up SQS queues
  // Using SQS queues I could have a lambda only for image resizing, and a lambda only for
  // putting objects in s3
  // S3Put -> This lambda -> creates jobs for each image to resize -> diff lambda processes, creates job to upload resized to s3 -> anther lambda reads
  // For this V1, this is an optimistic view that works

  const s3Uploads = [];
  console.time('resize, convert');
  await Promise.all(
    outputPhotoSizes.map(async (photoSize) => {
      console.log('working through size: ', photoSize);
      const resized = await startingImage
        .clone()
        .resize({ fit: 'inside', width: photoSize.width });

      await Promise.all(
        outputFormats.map(async (format) => {
          console.log('working through format', format);
          console.time(`converted ${imgName} at ${photoSize.prefix} to ${format.format}`);
          const imgBuffer = await convertImageToFormatAndReturnBuffer(
            resized.clone(),
            format
          );
          console.timeEnd(
            `converted ${imgName} at ${photoSize.prefix} to ${format.format}`
          );
          if (imgBuffer instanceof Error) return Promise.reject(imgBuffer);
          console.time(
            `upload to s3 for ${imgName} at ${photoSize.prefix} to ${format.format}`
          );
          s3Uploads.push(
            uploadImageToS3(
              imgBuffer,
              imgName,
              format.format,
              photoSize.prefix,
              imgDir,
              process.env.RESIZED_PHOTOS_BUCKET
            )
          );
          return Promise.resolve();
        })
      )
        .then(() => {
          console.log(`finished formatting formats at ${photoSize.prefix}`);
        })
        .catch((err) => {
          console.error(err);
          return err;
        });
      return Promise.resolve();
    })
  );
  console.timeEnd('resize, convert');

  await Promise.all(s3Uploads)
    .catch((err) => {
      console.log(err);
      return err;
    })
    .finally(console.log('finished all s3 uploads'));

  // Once all this is complete, append to the database
  // Im kinda assuming which sizes are going to be available, that's ok for now
  // same for the formats. Later when this is more error complete, I'll append to the database
  // only when I know all formats and sizes have been generated.
  const query = {
    text:
      'INSERT INTO photos_meta(dir_path, name, year, album_name, subalbum_name, width, height, alt_text, available_formats) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)',
    values: [
      imgDir,
      imgName,
      year,
      album,
      subAlbum,
      width,
      height,
      altText,
      ['avif', 'webp', 'jpeg'],
    ],
  };
  console.time('db insert');
  await client
    .query(query)
    .then((res) => console.log(res))
    .catch((err) => {
      console.error(err.stack);
      return Promise.reject('could not do db insert');
    })
    .finally(() => {
      client.release();
      console.timeEnd('db insert');
    });
  return Promise.resolve('resized, formatted, and inserted into db successfully');
};
