const AWS = require('aws-sdk')
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
    console.log(err)
    return new Error('failed to download original image');
  }
}

async function resizeImage(photoWidth) { }
async function uploadImageToS3() { } 

const PhotoSizes = {
  "": { Width: 333 },
  "@2x": { Width: 667 },
  "_large": { Width: 1500 },
  "_large@2x": { Width: 3000 }
}

/**
 * 
 * @param {*} event an S3 Event Notification https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
 */
exports.handler = async (event, context, callback) => {
  console.log(event);
  event.records.forEach(record => {
    const srcBucket = record.s3.bucket.name;
    const srcKey = record.s3.object.key; 

    const originalImage = await getOriginalImage(srcBucket, srcKey);
    if (originalImage instanceof Error) return;

    // Resize the image into each of the desired sizes
     Object.keys(PhotoSizes).forEach(key => {
       const resized = resizeImage(PhotoSizes[key].Width);
       if (resized instanceof Error) return;
       uploadImageToS3(resized);
    }

  })
}