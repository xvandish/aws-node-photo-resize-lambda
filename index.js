const AWS = require("aws-sdk");
const sharp = require("sharp");
const util = require("util");
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
    return new Error("failed to download original image");
  }
}

async function resizeImage(photoWidth) {}
async function uploadImageToS3() {}

const photoSizes = [
  { prefix: "small", width: 333 },
  { prefix: "@2x", width: 667 },
  { prefix: "_large", width: 1500 },
  { prefix: "_large@2x", Width: 3000 },
];

const supportedFormats = ["jpeg", "jpg", "png"];

/**
 *
 * @param {*} event an S3 Event Notification https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
 */
exports.handler = async (event, context, callback) => {
  console.log("Reading options from event:\n", util.inspect(event, { depth: 5 }));
  const record = event.Records[0];
  const srcBucket = record.s3.bucket.name;
  const srcKey = record.s3.object.key;

  // Get the extension from the file name
  const typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.log("Could not determine the image type.");
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

  console.time("create initial sharp instance");
  // Add rotation because even with EXIF metadata images come out roatated
  // without it
  const startingImage = await sharp(originalImage.Body).rotate();
  console.timeEnd("create initial sharp instance");

  photoSizes.forEach(async (key) => {
    const resized = resizeImage(PhotoSizes[key].Width);
    if (resized instanceof Error) return;
    uploadImageToS3(resized);
    s3.upload;
  });
};
