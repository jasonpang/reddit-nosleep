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
    ['url', 'TEXT'],
    ['author', 'TEXT']
]);

program
    .requiredOption('-i, --input <file>', 'Input JSON file path')
    .requiredOption('-o, --output <file>', 'Output SQLite database file path')
    .option('-t, --table <name>', 'Table name in SQLite database', 'data')
    .option('-b, --batch-size <size>', 'Number of records to process in each batch', '10000')
    .parse(process.argv);

const options = program.opts();
const BATCH_SIZE = parseInt(options.batchSize, 10);

console.log('📚 Configuration:');
console.log(`   📥 Input: ${options.input}`);
console.log(`   📦 Output: ${options.output}`);
console.log(`   📋 Table: ${options.table}`);
console.log(`   🔄 Batch size: ${BATCH_SIZE}`);

async function processData() {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(options.output);
        let processCount = 0;
        
        // Create table
        const schema = Array.from(COLUMNS.entries())
            .map(([name, type]) => `"${name}" ${type}`)
            .join(', ');
        
        console.log('\n🔨 Initializing database structures...');
        db.exec(`DROP TABLE IF EXISTS "${options.table}"`);
        db.exec(`CREATE TABLE "${options.table}" (${schema})`);

        const columnNames = Array.from(COLUMNS.keys());
        const stmt = db.prepare(
            `INSERT INTO "${options.table}" ("${columnNames.join('", "')}") 
             VALUES (${Array(columnNames.length).fill('?').join(', ')})`
        );

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
                
                if (processCount % BATCH_SIZE === 0) {
                    db.exec('COMMIT');
                    db.exec('BEGIN TRANSACTION');
                    console.log(`✨ Processed ${processCount.toLocaleString()} records`);
                }
            } catch (err) {
                console.error('❌ Error processing record:', err);
            }
        });

        rl.on('close', () => {
            stmt.finalize();
            db.exec('COMMIT');
            
            console.log(`\n🎉 Success! Processed ${processCount.toLocaleString()} records`);
            
            db.close((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(processCount);
                }
            });
        });

        rl.on('error', (err) => {
            console.error('❌ Fatal error:', err);
            rl.close();
            stmt.finalize();
            db.exec('ROLLBACK');
            db.close((closeErr) => {
                reject(err || closeErr);
            });
        });
    });
}

async function main() {
    try {
        const count = await processData();
        console.log('✨ All done! Database is ready.');
        process.exit(0);
    } catch (err) {
        console.error('❌ Fatal error:', err);
        process.exit(1);
    }
}

main();