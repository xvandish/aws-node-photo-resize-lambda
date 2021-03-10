const AWS = require('aws-sdk');
const sharp = require('sharp');
const util = require('util');
const s3 = new AWS.S3();

/**
 *
 * @param {string} bucket - name of the bucket
 * @param {string} key - name of the key
 * @param {s3} s3 - instantiated s3 object
 */
async function getOriginalImage(bucket, key) {
  const imageLocation = { Bucket: bucket, Key: key };
  try {
    const originalImage = await s3.getObject(imageLocation).promise();
    return originalImage;
  } catch (err) {
    console.log(err);
    return new Error('failed to download original image');
  }
}

/**
 *
 * @param {sharp.Sharp} sharpImage
 * @param {*} desiredFormat
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
  return s3
    .putObject({
      Bucket: bucketName,
      Key: `${imgDir}${imgName}${imgSizePrefix}.${imgFormat}`,
      Body: imgBytes,
      ContentType: `image/${imgFormat}`,
    })
    .promise();
}

const outputPhotoSizes = [
  { prefix: 'small', width: 333 },
  { prefix: '@2x', width: 667 },
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
exports.handler = async (event) => {
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

  const originalImage = await getOriginalImage(srcBucket, srcKey);
  if (originalImage instanceof Error) return;

  console.time('create initial sharp instance');
  // Add rotation because even with EXIF metadata images come out with the wrong orientation
  const startingImage = await sharp(originalImage.Body).rotate();
  console.timeEnd('create initial sharp instance');

  const parts = srcKey.split('/'); // 2020/nyc/manhattan_valley/thing.jpeg -> [2020, nyc, manhattan_valley, thing.jpeg]
  const imgName = parts[parts.length - 1].replace(typeMatch, '');
  const imgDir = `${parts.slice(0, -1).join('/')}/`;

  await Promise.all(
    outputPhotoSizes.map(async (photoSize) => {
      console.log('working through size: ', photoSize);
      const resized = await startingImage
        .clone()
        .resize({ fit: 'inside', width: photoSize.width });

      await Promise.all(
        outputFormats.map(async (format) => {
          console.log('working through format', format);
          const imgBuffer = await convertImageToFormatAndReturnBuffer(
            resized.clone(),
            format
          );
          if (imgBuffer instanceof Error) return Promise.reject(imgBuffer);
          return uploadImageToS3(
            imgBuffer,
            imgName,
            format.format,
            photoSize.prefix,
            imgDir,
            process.env.RESIZED_PHOTOS_BUCKET
          );
        })
      ).catch((err) => {
        console.log(err);
        return;
      });
    })
  );
};
