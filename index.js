// Import axios, csv, and fs modules:
const axios = require("axios");
const fs = require("fs").promises;
const { parse } = require("csv-parse");
const { stringify } = require("csv-stringify");

// Store the urls in an array:
let urls = [
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group01.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group02.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group03.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group04.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group05.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group06.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group07.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group08.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group09.csv`,
  `https://dose-health-coding-challenge.s3.amazonaws.com/Group10.csv`,
];

// Fetch the file data as a stream:
const fetchFile = async (url) => {
  try {
    const { data } = await axios.get(url, { responseType: "stream" });
    return data;
  } catch (error) {
    console.error(error);
  }
};

// Initiate an array to hold all data from the fetched files:
let allEntries = [];

// Fetch the data and compile all of the records in the allEntries array:
const parseData = async (url) => {
  let fileData = await fetchFile(url);
  return new Promise((resolve) => {
    fileData
      .pipe(
        parse({ columns: true }, (error, records) => {
          if (error) {
            console.error(error);
          } else {
            allEntries = [...allEntries, ...records];
          }
        })
      )
      .on("end", () => {
        resolve(allEntries);
      });
  });
};

// Convert to an array form that is easy to convert to CSV format:
const convertToArray = (object) => {
  let result = [];
  for (let key in object) {
    result.push({ zipCode: key, count: object[key] });
  }
  return result;
};

// Get the parsed data, compute frequencies, and return the frequencies:
const countZipFrequency = async () => {
  for (let i = 0; i < urls.length; i++) {
    await parseData(urls[i]);
  }

  // Compute frequencies using a hash map, implemented as a JavaScript object:
  let zipsCount = {};
  for (let i = 0; i < allEntries.length; i++) {
    zipsCount[allEntries[i].ZipCode] =
      (zipsCount[allEntries[i].ZipCode] || 0) + 1;
  }

  // Convert to an array format to later "stringify" for a csv file:
  return convertToArray(zipsCount);
};

// Write a new CSV file to store the zip codes & their frequencies:
const writeNewCsv = async () => {
  let zipsCountArray = await countZipFrequency();
  stringify(zipsCountArray, { header: true }, (error, output) => {
    if (error) {
      console.error(error);
    } else {
      fs.writeFile("./zip_frequencies.csv", output);
    }
  });
};

// Call the function to write the summary CSV file:
writeNewCsv();
