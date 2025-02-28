import { LuffyDBClient } from './client';

async function main(){
    const client = new LuffyDBClient(undefined, false, 'mondluffy');

    await client.defineTable("stress_test", ['id', 'name'])

    let qt = await client.query("stress_test");

    console.log(await client.listTables())

    console.log(qt);

    client.cleanBackup("stress_test");

    // for(const q of qt)  await client.delete("stress_test", q['id'])
    // client.insert("stress_test", {
    //     name: `MON D. LUFFY`,
    //     age: 19,
    //     email: `user@example.com`,
    //     bio: `Bio for user with some lengthy text to stress the system`,
    //     status: 'active',
    //     score: 10
    // })

    // await client.update("stress_test", "588316da-ec3a-4b73-b231-c0f500b91efe", {
    //     score: 224
    // })
}

main();