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

let currentJob = null;
const etcd = new Etcd();
etcd.init({ etcd: etcdOptions, serviceName });
etcd.jobs.on('change', (res) => {
    console.log(`job stopped ${currentJob.id}. result: ${JSON.stringify(res)}`);
    currentJob.done(null);
});

const consumer = new Consumer(setting);
consumer.on('job', (job) => {
    console.log(`job arrived for ${job.data.node} with input: ${JSON.stringify(job.data.input)}`);
    etcd.jobs.watch({ jobId: job.data.jobID });

    currentJob = job;
    setTimeout(async () => {
        const rand = random();
        const result = rand;

        if (rand > 0) {
            console.log(`job ${job.id} done with ${JSON.stringify(result)}`);
            await etcd.tasks.setState({ jobId: job.data.jobID, taskId: job.id, result: result, status: 'completed' });
            job.done(null, result);
        }
        else {
            const error = new Error('some strange error');
            console.log(`job ${job.id} failed with ${JSON.stringify(result)}`);
            await etcd.tasks.setState({ jobId: job.data.jobID, taskId: job.id, error: error.message, status: 'failed' });
            job.done(error);
        }
    }, 50000);
});

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



