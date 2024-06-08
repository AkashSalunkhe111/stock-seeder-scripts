const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const neo4j = require('neo4j-driver');

function parseToInt(str) {
    const result = parseInt(str, 10);
    if(str === 'New') {
        return 9999
    }
    if(str === 'Below 1%') {
        return -9999
    }
    if(str === 'Filing Awaited') {
        return -9999
    }
    if(str === '-') {
        return 0
    }
    if (isNaN(result)) {
      throw new Error(`Invalid number: ${str}`);
    }
    return result;
  }

  function extractName(fileName) {
    // Use a regular expression to match the desired part of the string
    const regex = /^Latest\s+(.*?)\s+shareholdings\s+and\s+portfolio\.csv$/i;
    const match = fileName.match(regex);
    
    // Check if the regular expression matched any part of the string
    if (match && match[1]) {
      return match[1];
    } else {
      throw new Error("Invalid file name format");
    }
  }

// Neo4j connection configuration
const uri = 'neo4j://localhost:7687'; // Neo4j Bolt protocol
const user = 'neo4j'; // Neo4j username
const password = 'Akash@123'; // Neo4j password
const database = 'neo4j';

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
const session = driver.session({ database });

// Folder containing CSV files
const csvFolderPath = './data';

const importDataFromCSV = async (filePath, superStarName) => {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const parsedData = Papa.parse(fileContent, { header: true }).data;

    for (const row of parsedData) {
        const tags = [];
        const stockName = row['Stock'];
        const latestChange = row['Mar 2024 Change %'];
        const latestHolding = row['Mar 2024  Holding %'];
        if(latestChange === 'New') {
            tags.push('New');
        }
        if(latestChange === 'Below 1%'){
            tags.push('Exited');
        }
        if(latestHolding === 'Filing Awaited'){
            tags.push('Filing Awaited');
        }

        const latestChangePrepared = parseToInt(latestChange);
        const latestHoldingPrepared = parseToInt(latestHolding)

        console.log(Object.keys(row));
        console.log('Stock Name ----> ', row['Stock']);
        console.log('Mar 2024 Change %', parseToInt(row['Mar 2024 Change %']));

        console.log('Mar 2024  Holding %', parseToInt(row['Mar 2024  Holding %']))

        await session.run(
            `MERGE (s:SuperStar {name: $superStarName})
             MERGE (st:Stock {name: $stockName})
             CREATE (s)-[:HAS_TRANSACTION {
               latestChange: $latestChangePrepared, 
               latestHolding: $latestHoldingPrepared
             }]->(st)
             WITH s, st
             UNWIND $tags AS tag
             MERGE (t:Tag {name: tag})
             MERGE (s)-[:TAGGED_AS]->(t)`,
            { superStarName, stockName, latestChangePrepared, latestHoldingPrepared, tags }
          );
    }

    console.log(`Data import from ${filePath} complete!`);
  } catch (error) {
    console.error(`Error importing data from ${filePath}:`, error);
  }
};

const importDataFromFolder = async () => {
  try {
    const files = fs.readdirSync(csvFolderPath);
    for (const file of files) {
      if (path.extname(file) === '.csv') {
        const filePath = path.join(csvFolderPath, file);
        await importDataFromCSV(filePath,extractName(file).trim());
      }
    }
  } catch (error) {
    console.error('Error reading files from folder:', error);
  } finally {
    await session.close();
    await driver.close();
  }
};

importDataFromFolder();
