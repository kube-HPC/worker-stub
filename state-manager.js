const Etcd = require('etcd.rf');

class StateManager {

    constructor() {
        const etcd = {
            protocol: 'http',
            host: process.env.ETCD_SERVICE_HOST || 'localhost',
            port: process.env.ETCD_SERVICE_PORT || 4001
        };
        const serviceName = 'worker-stub';
        this._etcd = new Etcd();
        this._etcd.init({ etcd, serviceName });
    }

    async update(options) {
        await Promise.all([
            this._updateResult(options),
            this._updateStatus(options)
        ]);
    }

    async _updateResult(options) {
        await this._etcd.tasks.setResult(options);
    }

    async _updateStatus(options) {
        await this._etcd.tasks.setStatus(options);
    }
}

module.exports = new StateManager();
