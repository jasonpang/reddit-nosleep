#!/usr/bin/env node

import { program } from 'commander';
import sqlite3 from 'sqlite3';
import fs from 'fs';
import readline from 'readline';

const COLUMNS = new Map([
    ['created_utc', 'DATETIME'],
    ['num_comments', 'NUMERIC'],
    ['permalink', 'TEXT'],
    ['score', 'NUMERIC'],
    ['selftext', 'TEXT'],
    ['title', 'TEXT'],
    ['url', 'TEXT']
]);

program
    .requiredOption('-i, --input <file>', 'Input JSON file path')
    .requiredOption('-o, --output <file>', 'Output SQLite database file path')
    .option('-t, --table <name>', 'Table name in SQLite database', 'data')
    .option('-b, --batch-size <size>', 'Number of records to process in each batch', '1000')
    .parse(process.argv);

const options = program.opts();
const BATCH_SIZE = parseInt(options.batchSize, 10);

console.log('Input file:', options.input);
console.log('Output file:', options.output);
console.log('Table name:', options.table);
console.log('Batch size:', BATCH_SIZE);

async function processData() {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(options.output);
        let processCount = 0;
        let batch = [];

        const schema = Array.from(COLUMNS.entries())
            .map(([name, type]) => `"${name}" ${type}`)
            .join(', ');
        
        console.log('\nCreating table with schema:');
        const createTableSQL = `CREATE TABLE IF NOT EXISTS "${options.table}" (${schema})`;
        console.log('SQL:', createTableSQL);
        db.exec(createTableSQL);
        
        const columnNames = Array.from(COLUMNS.keys());
        const insertSQL = `INSERT INTO "${options.table}" ("${columnNames.join('", "')}") VALUES (${Array(columnNames.length).fill('?').join(', ')})`;
        console.log('Prepared SQL:', insertSQL);
        const stmt = db.prepare(insertSQL);

        db.exec('BEGIN TRANSACTION');

        const rl = readline.createInterface({
            input: fs.createReadStream(options.input),
            crlfDelay: Infinity
        });

        rl.on('line', (line) => {
            if (!line.trim()) return;

            try {
                const record = JSON.parse(line);
                const values = columnNames.map(key => record[key] ?? null);
                stmt.run(values);
                processCount++;

                if (processCount % 10000 === 0) {
                    db.exec('COMMIT');
                    db.exec('BEGIN TRANSACTION');
                    console.log(`Processed ${processCount} records`);
                }
            } catch (err) {
                console.error('Error processing line:', err);
            }
        });

        rl.on('close', () => {
            stmt.finalize();
            db.exec('COMMIT');
            console.log(`\nComplete. Processed ${processCount} records`);
            db.close();
            resolve();
        });

        rl.on('error', (err) => {
            console.error('Reader error:', err);
            stmt.finalize();
            db.exec('ROLLBACK');
            db.close();
            reject(err);
        });
    });
}

async function main() {
    try {
        await processData();
        console.log('All done!');
    } catch (err) {
        console.error('Error:', err);
        process.exit(1);
    }
}

main();