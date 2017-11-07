
const { Consumer } = require('producer-consumer.rf');
const state = require('./state-manager');

const setting = {
    job: {
        type: process.env.ALG || 'green-alg'
    },
    setting: {
        prefix: 'jobs-workers'
    }
};
const c = new Consumer(setting);
c.on('job', (job) => {
    console.log(`job arrived for ${job.data.node} with input: ${JSON.stringify(job.data.input)}`);

    setTimeout(async () => {
        const rand = random();
        const result = rand;

        if (rand > 0) {
            console.log(`job ${job.id} done with ${JSON.stringify(result)}`);
            await state.update({ jobId: job.data.jobID, taskId: job.id, result: result, status: 'completed' });
            job.done(null, result);
        }
        else {
            const error = new Error('failed');
            console.log(`job ${job.id} failed with ${JSON.stringify(result)}`);
            await state.update({ jobId: job.data.jobID, taskId: job.id, error: error.message, status: 'failed' });
            job.done(error);
        }
    }, 100);
});

c.register(setting);
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



