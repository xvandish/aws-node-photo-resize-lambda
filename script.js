const sharp = require('sharp');

// Testing compression for images, to see if either webp or avif are worth it

const PhotoSizes = {
  '': { Width: 333 },
  '@2x': { Width: 667 },
  _large: { Width: 1500 },
  '_large@2x': { Width: 3000 },
};
const fileName = 'soak';
async function test() {
  console.time('pipeline init');
  const pipeline = await sharp(`${fileName}.jpg`).rotate();
  console.timeEnd('pipeline init');
  Object.keys(PhotoSizes).forEach(async (key) => {
    const width = PhotoSizes[key].Width;
    console.time(`convert image ${key}`);
    pipeline.resize(width).webp({ quality: 55 }).toFile(`${fileName}${key}.webp`);
    console.timeEnd(`convert image ${key}`);
  });
}

console.time('whole');
test();
console.timeEnd('whole');
