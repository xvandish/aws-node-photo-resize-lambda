const AWS = require('aws-sdk');
const sharp = require('sharp');
const util = require('util');
const s3 = new AWS.S3();

AWS.config.update({region: 'us-west-2'});
// Create an SQS service object
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
var queueURL = process.env.IMAGE_META_TO_PG_QUEUE_URL;

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

function deletePartialResults(imgName, imgDir) {
  return s3
    .deleteObjects({
      Bucket: process.env.RESIZED_PHOTOS_BUCKET,
      Delete: {
        Objects: outputPhotoSizes
          .map((size) =>
            outputFormats.map((format) => ({
              Key: `${imgDir}${imgName}${size.prefix}.${format.format}`,
            }))
          )
          .flat(1),
      },
    })
    .promise()
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
 * 5. Send a notification to SQS that this result needs to be added to a db. Another lambda will insert
 */
exports.handler = async (event, context) => {
  console.log('Reading options from event:\n', util.inspect(event, { depth: 5 }));
  const record = event.Records[0];
  const srcBucket = record.s3.bucket.name;
  // Object key may have spaces or unicode non-ASCII characters.
  const srcKey    = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "))

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

  let originalImage;
  let startingImage;
  try {
    originalImage = await s3.getObject({ Bucket: srcBucket, Key: srcKey }).promise();
    // Add rotation because even if the EXIF metadata of the final images
    // mentions the correct orientation, it isn't applied automatically
    startingImage = await sharp(originalImage.Body).rotate();
  } catch (err) {
    console.error(err)
    return Promise.reject(err);
  }

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
  // partial results are useless and I can't track down errors easily right now
  // generate all buffers first. 
  // then start s3 uploads

  for (var i = 0; i < outputPhotoSizes.length; i++) {
    const photoSize = outputPhotoSizes[i];

    let resized;
    try {
      resized = await startingImage
      .clone()
      .resize({ fit: 'inside', width: photoSize.width })


      for (var j = 0; j < outputFormats.length; j++) {
        const format = outputFormats[j];
        console.time(`converted ${imgName} at ${photoSize.prefix} to ${format.format}`);
        const imgBuffer = await resized
        .clone()
        .toFormat(format.format, { quality: format.quality, })
        .toBuffer()
        console.timeEnd(`converted ${imgName} at ${photoSize.prefix} to ${format.format}`);

        // push a function that calls our function, so we can execute this later
        s3Uploads.push(() =>
          uploadImageToS3(
            imgBuffer,
            imgName,
            format.format,
            photoSize.prefix,
            imgDir,
            process.env.RESIZED_PHOTOS_BUCKET
          )
        )
      }
    } catch (err) {
      console.error(err);
      return Promise.reject(err);
    }
  }

  
  try {
    await Promise.all(s3Uploads.map(future => future()));
  } catch (err) {
    console.error(err);
    try {
      // Try to clean up any uploads that we now can't use
      await deletePartialResults(imgName, imgDir);
      console.log('partial uploads deleted')
    } catch (nestedErr) {
      console.error(nestedErr);
      return Promise.reject(err)
    }
    
  }
 
  // Once all this is complete, append to the database
  // Im kinda assuming which sizes are going to be available, that's ok for now
  // same for the formats. Later when this is more error complete, I'll append to the database
  // only when I know all formats and sizes have been generated.

  const query = {
    text:
      'INSERT INTO photos_meta(dir_path, name, year, album_name, subalbum_name, width, height, alt_text, available_formats) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING',
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

  // Create publish parameters
  const params = {
    MessageBody: JSON.stringify(query),
    QueueUrl: queueURL
  };

  try {
    const x = await sqs.sendMessage(params).promise();
    console.log('published to sqs fine - ', x);
  } catch (err) {
    console.log(err);
    return err;
  }
};
