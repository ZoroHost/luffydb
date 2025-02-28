import { LuffyDBClient } from './client';

async function stressBenchmark() {
  const client = new LuffyDBClient(undefined, false);
  const tableName = 'stress_test';
  const totalRows = 20; // 10K rows for insertion
  const concurrentOps = 50; // Number of concurrent operations

  console.log("Starting stress benchmark...");
  console.time('Stress Benchmark Total');

  // Define the table with extended columns
  try {
    console.time('Define Table');
    await client.defineTable(tableName, ['name', 'age', 'email', 'bio', 'status', 'score']);
    console.timeEnd('Define Table');
  } catch (error) {
    console.error("Error during table definition:", error);
  }

  // Insert 10K rows concurrently
  let rowIds: string[] = [];
  try {
    console.time('Insert 10K Rows');
    const insertPromises = Array.from({ length: totalRows }, (_, i) =>
      client.insert(tableName, {
        name: `User${i}`,
        age: (18 + (i % 60)).toString(),
        email: `user${i}@example.com`,
        bio: `Bio for user ${i} with some lengthy text to stress the system`,
        status: i % 2 === 0 ? 'active' : 'inactive',
        score: (Math.random() * 100).toFixed(2),
      })
    );
    rowIds = await Promise.all(insertPromises);
    console.timeEnd('Insert 10K Rows');
    console.log(`Inserted ${rowIds.length} rows.`);
  } catch (error) {
    console.error("Error inserting rows:", error);
  }

  // Query all rows after insertion
  try {
    console.time('Query All Rows');
    const allRows = await client.query(tableName);
    console.log(`Total rows after insertion: ${allRows.length}`);
    console.timeEnd('Query All Rows');
  } catch (error) {
    console.error("Error querying all rows:", error);
  }

  // Execute concurrent LIKE queries with LIMIT
  try {
    console.time('Concurrent LIKE Queries');
    const likeQueries = Array.from({ length: concurrentOps }, (_, i) =>
      client.query(tableName, {}, 100, { name: `User${i * 100}` })
    );
    const likeResults = await Promise.all(likeQueries);
    const totalLikeResults = likeResults.reduce((sum, arr) => sum + arr.length, 0);
    const averageLikeResults = totalLikeResults / concurrentOps;
    console.log(`Average LIKE query returned ${averageLikeResults} rows.`);
    console.timeEnd('Concurrent LIKE Queries');
  } catch (error) {
    console.error("Error during concurrent LIKE queries:", error);
  }

  // Perform concurrent updates on the first set of inserted rows
  try {
    console.time('Concurrent Updates');
    const updatePromises = Array.from({ length: concurrentOps }, (_, i) =>
      client.update(tableName, rowIds[i], {
        score: (Math.random() * 100).toFixed(2),
        status: 'updated',
      })
    );
    await Promise.all(updatePromises);
    console.timeEnd('Concurrent Updates');
    console.log(`Performed ${concurrentOps} concurrent updates.`);
  } catch (error) {
    console.error("Error during concurrent updates:", error);
  }

  // Perform concurrent deletes on a different set of rows
  try {
    console.time('Concurrent Deletes');
    const deletePromises = Array.from({ length: concurrentOps }, (_, i) =>
      client.delete(tableName, rowIds[i + concurrentOps])
    );
    await Promise.all(deletePromises);
    console.timeEnd('Concurrent Deletes');
    console.log(`Performed ${concurrentOps} concurrent deletes.`);
  } catch (error) {
    console.error("Error during concurrent deletes:", error);
  }

  // Final query to assess state after modifications
  try {
    console.time('Query After Mods');
    const remainingRows = await client.query(tableName, { status: 'active' }, 1000);
    console.log(`Active rows after modifications: ${remainingRows.length}`);
    console.timeEnd('Query After Mods');
  } catch (error) {
    console.error("Error querying rows after modifications:", error);
  }

  console.timeEnd('Stress Benchmark Total');
  console.log("Stress benchmark completed.");
}

stressBenchmark().catch(console.error);
