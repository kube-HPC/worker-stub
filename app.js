const uuidv4 = require('uuid/v4');
const Etcd = require('@hkube/etcd');
const { Consumer } = require('@hkube/producer-consumer');

const serviceName = 'workers';
const setting = {
    job: {
        type: process.env.ALG || 'green-alg'
    },
    setting: {
        prefix: 'jobs-workers',
        settings: {
            lockDuration: 10000,
            stalledInterval: 10000,
            maxStalledCount: 1
        }
    }
};
const etcdOptions = {
    protocol: 'http',
    host: process.env.ETCD_SERVICE_HOST || 'localhost',
    port: process.env.ETCD_SERVICE_PORT || 4001
};

let jobId = null;
let taskId = null;
let pipelineName = null;

const useRandomTimeout = false;
const useFixedTimeout = true;
const randomBatchFail = false;
const fixedTimeout = 1000;
let currentJob = null;

const etcd = new Etcd();
async function init() {
    etcd.init({ etcd: etcdOptions, serviceName });
    await etcd.discovery.register({ serviceName });

    setInterval(() => {
        updateDiscovery({ status: 'active' });
    }, 2000)

    start();
}

function start() {
    etcd.jobs.on('change', (res) => {
        console.log(`job stopped ${jobId}. result: ${JSON.stringify(res)}`);
        etcd.jobs.unwatch({ jobId });
        currentJob.done();
    });

    const consumer = new Consumer(setting);
    consumer.on('job', (job) => {

        jobId = job.data.jobID;
        taskId = job.data.taskID;
        pipelineName = job.data.pipeline_name;

        updateDiscovery({ status: 'active', ...job.data });

        etcd.jobs.watch({ jobId });
        currentJob = job;
        console.log(`job arrived for node ${job.data.nodeName} with input: ${JSON.stringify(job.data.input)}`);
        etcd.tasks.setState({ jobId, taskId, status: 'active' });

        if (useRandomTimeout || useFixedTimeout) {
            const timeout = useFixedTimeout ? fixedTimeout : random() * 1000;
            console.log(`starting timeout of ${timeout / 1000} sec`);

            setTimeout(() => {
                endJob(job);
            }, timeout);
        }
        else {
            endJob(job);
        }
    });

    consumer.register(setting);
    console.log(`worker ready for algo ${setting.job.type}`);
}

async function updateDiscovery(data) {
    return etcd.discovery.updateRegisteredData({
        jobId: jobId,
        taskId: taskId,
        pipelineName: pipelineName,
        algorithmName: setting.job.type,
        ...data
    });
}

async function endJob(job) {
    const isBatch = job.data.batchIndex >= 0;
    const rand = isBatch && randomBatchFail ? random() : 20;
    const input = job.data.input[0];

    if (input > 10) {
        const error = new Error('some strange error');
        await etcd.tasks.setState({ jobId, taskId, error: error.message, status: 'failed' });
        await etcd.jobs.unwatch({ jobId });
        job.done(error);
        console.log(`job ${job.id} failed with ${error.message}`);
    }
    else {
        await etcd.tasks.setState({ jobId, taskId, result: input, status: 'succeed' });
        await etcd.jobs.unwatch({ jobId });
        job.done();
        console.log(`job ${job.id} succeed with ${input}`);
    }
}

function random() {
    return Math.floor(Math.random() * 10) + 1
}

init();

_handleErrors();

function _handleErrors() {
    process.on('exit', (code) => {
        console.log('exit' + (code ? ' code ' + code : ''));
    });
    process.on('SIGINT', () => {
        console.log('SIGINT');
        process.exit(1);
    });
    process.on('SIGTERM', () => {
        console.log('SIGTERM');
        process.exit(1);
    });
    process.on('unhandledRejection', (error) => {
        console.log('unhandledRejection: ' + error.message);
        console.log(error);
    });
    process.on('uncaughtException', (error) => {
        console.log('uncaughtException: ' + error.message);
        process.exit(1);
    });
}



