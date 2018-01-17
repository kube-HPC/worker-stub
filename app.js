const Etcd = require('@hkube/etcd');
const { Consumer } = require('@hkube/producer-consumer');

const serviceName = 'worker-stub';
const setting = {
    job: {
        type: process.env.ALG || 'green-alg'
    },
    setting: {
        prefix: 'jobs-workers'
    }
};
const etcdOptions = {
    protocol: 'http',
    host: process.env.ETCD_SERVICE_HOST || 'localhost',
    port: process.env.ETCD_SERVICE_PORT || 4001
};

const useRandomTimeout = false;
const useFixedTimeout = true;
const randomBatchFail = false;
const fixedTimeout = 1000;

let currentJob = null;
const etcd = new Etcd();
etcd.init({ etcd: etcdOptions, serviceName });
etcd.jobs.on('change', (res) => {
    console.log(`job stopped ${currentJob.id}. result: ${JSON.stringify(res)}`);
    etcd.jobs.unwatch({ jobId: job.data.jobID });
    currentJob.done();
});

const consumer = new Consumer(setting);
consumer.on('job', (job) => {
    etcd.jobs.watch({ jobId: job.data.jobID });
    currentJob = job;
    console.log(`job arrived for node ${job.data.node} with input: ${JSON.stringify(job.data.input)}`);
    etcd.tasks.setState({ jobId: job.data.jobID, taskId: job.data.taskID, status: 'active' });

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

    const e1 = {
        "nodeName": "black",
        "batchID": "black#7",
        "algorithmName": "black-alg",
        "error": {
            "code": 33,
            "message": 'some error',
            "details": ''
        }
    }

    const e2 = {
        "nodeName": "black",
        "batchID": "black#7",
        "algorithmName": "black-alg",
        "result": 42
    }
});

async function endJob(job) {
    const isBatch = job.data.node.indexOf('#') > 0
    const rand = isBatch && randomBatchFail ? random() : 20;
    const result = job.data.input[0];

    if (rand > 5) {
        await etcd.tasks.setState({ jobId: job.data.jobID, taskId: job.data.taskID, result: result, status: 'succeed' });
        await etcd.jobs.unwatch({ jobId: job.data.jobID });
        job.done();
        console.log(`job ${job.id} succeed with ${result}`);
    }
    else {
        const error = new Error('some strange error');
        await etcd.tasks.setState({ jobId: job.data.jobID, taskId: job.data.taskID, error: error.message, status: 'failed' });
        await etcd.jobs.unwatch({ jobId: job.data.jobID });
        job.done(error);
        console.log(`job ${job.id} failed with ${error.message}`);
    }
}

consumer.register(setting);
console.log(`worker ready for algo ${setting.job.type}`);

function random() {
    return Math.floor(Math.random() * 10) + 1
}

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



