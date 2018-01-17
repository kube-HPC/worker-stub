const { Etcd3 } = require('etcd3');
const uuidv4 = require('uuid/v4');
const path = `path-${uuidv4()}`;
const client = new Etcd3({ hosts: 'http://localhost:4001' });
let watcher;

const watch = async (path) => {
    watcher = await client.watch().prefix(path).create();
}

const unwatch = async () => {
    await watcher.cancel();
}

const test = async () => {
    await watch(path);
    console.log(`success watch`);

    await unwatch();
    console.log(`success unwatch`);

    await watch(path); // this does not resolved
    console.log(`success watch`);

    await unwatch();
    console.log(`success unwatch`);

    await watch(path); // this does not resolved
    console.log(`success watch`);

    await unwatch();
    console.log(`success unwatch`);

    await watch(path); // this does not resolved
    console.log(`success watch`);
}

test();