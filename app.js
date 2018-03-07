const uuidv4 = require('uuid/v4');
const vm = require('vm');
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
const fixedTimeout = 10000;
let currentJob = null;

const etcd = new Etcd();

(async function init() {
    etcd.init({ etcd: etcdOptions, serviceName });
    await etcd.discovery.register({ serviceName });
    await updateDiscovery({ status: 'active' });
    start();
})();

async function updateDiscovery(data) {
    return etcd.discovery.updateRegisteredData({
        jobId: jobId,
        taskId: taskId,
        pipelineName: pipelineName,
        algorithmName: setting.job.type,
        ...data
    });
}

function start() {
    etcd.jobs.on('change', (res) => {
        console.log(`job stopped ${jobId}. result: ${JSON.stringify(res)}`);
        etcd.jobs.unwatch({ jobId });
        currentJob.done();
    });

    const consumer = new Consumer(setting);
    consumer.on('job', async (job) => {
        console.log(`job arrived for node ${job.data.nodeName} with input: ${JSON.stringify(job.data.input)}`);
        currentJob = job;
        jobId = job.data.jobID;
        taskId = job.data.taskID;
        pipelineName = job.data.pipelineName;
        await etcd.tasks.setState({ jobId, taskId, status: 'active' });
        const input = job.data.input[0];
        const code = job.data.info && job.data.info.extraData && job.data.info.extraData.code && job.data.extraData.code.join('\n');

        if (code) {
            const wrapper = (input) => {
                return new Promise((resolve, reject) => {
                    const userFunctionPromise = Promise.resolve(vm.runInThisContext(`(${code})`)(input));
                    userFunctionPromise.then((result) => {
                        return resolve(result);
                    }).catch(error => {
                        return reject(error);
                    })
                });
            }
            const output = await wrapper(job.data.input);
            endJob(output);
        }
        else {
            endJob(input);
        }
    });

    consumer.register(setting);
    console.log(`worker ready for algo ${setting.job.type}`);
}

async function endJob(output) {
    if (useRandomTimeout || useFixedTimeout) {
        const timeout = useFixedTimeout ? fixedTimeout : random() * 1000;
        console.log(`starting timeout of ${timeout / 1000} sec`);

        setTimeout(() => {
            _endJob(output);
        }, timeout);
    }
    else {
        _endJob(output);
    }
}

async function _endJob(output) {
    const isBatch = currentJob.data.batchIndex >= 0;
    const rand = isBatch && randomBatchFail ? random() : 20;

    if (rand < 20) {
        const error = new Error('some strange error');
        await etcd.tasks.setState({ jobId, taskId, error: error.message, status: 'failed' });
        currentJob.done(error);
        console.log(`job ${currentJob.id} failed with ${error.message}`);
    }
    else {
        await etcd.tasks.setState({ jobId, taskId, result: output, status: 'succeed' });
        currentJob.done();
        console.log(`job ${currentJob.id} succeed with ${output}`);
    }
}

function random() {
    return Math.floor(Math.random() * 10) + 1
}

(function _handleErrors() {
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
})();



