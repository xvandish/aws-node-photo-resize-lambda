const fs = require('fs')
const path = require('path')
const { S3Client, PutObjectCommand, ListObjectsCommand  } = require("@aws-sdk/client-s3");
const { Pool } = require('pg');
const fetch = require('node-fetch')
const parsePgConnectionString = require('pg-connection-string')

const herokuApiKey = process.env.HEROKU_API_KEY;
const herokuPostgresId = process.env.HEROKU_POSTGRES_ID;
let dbPool; // can't initialize until we have info

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

  console.log({ herokuConfig })

  const pgConfig = {
    ...parsePgConnectionString(herokuConfig[0].value), // the db string returned by heroku
      max: 1,
      ssl: {
        rejectUnauthorized: false
      }
  }
  dbPool = new Pool(pgConfig)
}

const REGION = "us-west-2"; //e.g. "us-east-1"
const s3Client = new S3Client({ region: REGION });


const photosDir = '/mnt/c/Users/rodri/Downloads/spainPictures/iCloud\ Photos/'
const commonPrefix = '2021/spain'

async function uploadFile(filePath, fileKey) {
    // Read content from the file
    try {
        // read in the file
        const data = await fs.promises.readFile(filePath);
        const bucketParams = {
            Bucket: "x",
            // Specify the name of the new object. For example, 'index.html'.
            // To create a directory for the object, use '/'. For example, 'myApp/package.json'.
            Key: fileKey,
            // Content of the new object.
            Body: data,
        }

        console.log('trying to upload ', fileKey)

        await s3Client.send(new PutObjectCommand(bucketParams));
        // return data; // For unit tests.
        console.log(
          "Successfully uploaded object: " +
            bucketParams.Bucket +
            "/" +
            bucketParams.Key
        );
      } catch (err) {
        console.log("Error", err);
      }
};

function uploadAll() {
    fs.readdir(photosDir, (err, folders) => {
        folders.forEach(folder => {
            fs.readdir(path.join(photosDir, folder), (err, files) => {
                files.forEach(file => {
                    uploadFile(path.join(photosDir, folder, file), `${commonPrefix}/${folder}/${file}`)
                })
            })
        })
    });
}

function stripExtension(fileKey) {
    const typeMatch = fileKey.match(/\.([^.]*)$/);
    const imgName = fileKey.replace(typeMatch[0], '')
    return imgName;
}

async function resolveDifferences() {
    const allKeys = [];
    fs.readdir(photosDir, (err, folders) => {
        folders.forEach(folder => {
            fs.readdir(path.join(photosDir, folder), (err, files) => {
                files.forEach(file => {
                    allKeys.push(`${commonPrefix}/${folder}/${file}`)
                })
            })
        })
    });

    const params = {
        Bucket: 'photos-989466858685-us-west-2',
        Prefix: '2021/spain'
    }
    const response = await s3Client.send(new ListObjectsCommand(params));

    const diff = allKeys.filter(x => !(response.Contents.find(el => el.Key === x)))
    console.log({ diff })

    if (!dbPool) {
        await initializePgPool();
    }

    const res = await dbPool.query('SELECT concat(dir_path, name) as key FROM photos_meta where year = 2021');
    console.log(res.rows[0])
    const secondDiff = response.Contents.filter(({ Key }) => !(res.rows.find(el => el.key.includes(stripExtension(Key)) )))
    console.log({ secondDiff })
    return Promise.resolve();
}

resolveDifferences();


// read every folder in photosDir
// for every folder, upload every file to s3 at the path
// 2021/spin/{folderName}/{fileName}


