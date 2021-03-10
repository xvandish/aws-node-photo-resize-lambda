const sharp = require("sharp");

// Testing compression for images, to see if either webp or avif are worth it

const PhotoSizes = {
  small: { Width: 333 },
  "@2x": { Width: 667 },
  _large: { Width: 1500 },
  "_large@2x": { Width: 3000 },
};
const fileName = "./IMG_1868.jpeg";
async function test() {
  console.time("pipeline init");
  const pipeline = await sharp(fileName).rotate();
  console.timeEnd("pipeline init");
  console.time("test-timer");
  Object.keys(PhotoSizes).forEach(async (key) => {
    const width = PhotoSizes[key].Width;
    console.time(`convert image to 3 formats ${key}`);
    const secondPipeline = pipeline.resize({
      fit: "inside",
      width,
    });
    await Promise.all([
      // secondPipeline
      //   .webp({ quality: 82 })
      //   .toFile(`./output_photos/${fileName}${key}.webp`),
      // secondPipeline.jpeg({ quality: 80 }).toFile(`./output_photos/${fileName}${key}.JPEG`),
      secondPipeline
        .avif({ quality: 64 })
        .toFile(`./output_photos/${fileName}${key}.avif`),
    ]);
    console.timeEnd(`convert image to 3 formats ${key}`);
  });
  console.timeEnd("test-timer");
}

test();
