const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const mediasoup = require('mediasoup');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    path: '/live/socket.io'
});

app.use(express.static('public'));

app.get('/live/:roomId', (req, res) => {
    res.sendFile(path.join(__dirname, './public/obs.html'));
});
app.get('/live', (req, res) => {
    res.sendFile(path.join(__dirname, './public/index.html'));
});
app.get('/private', (req, res) => {
    res.sendFile(path.join(__dirname, './public/call.html'));
});

// --- Konfigurasi Mediasoup ---
let worker;
let router;

const mediaCodecs = [
    {
        kind: 'audio',
        mimeType: 'audio/opus',
        clockRate: 48000,
        channels: 2
    },
    {
        kind: 'video',
        mimeType: 'video/VP8',
        clockRate: 90000,
        parameters: { 'x-google-start-bitrate': 1000 }
    }
];

async function createWorker() {
    worker = await mediasoup.createWorker({
        rtcMinPort: 10000,
        rtcMaxPort: 10100
    });

    worker.on('died', () => {
        console.error('Mediasoup worker died, exiting in 2 seconds... [PID:%d]', worker.pid);
        setTimeout(() => process.exit(1), 2000);
    });

    // Untuk skala kecil, 1 router sudah cukup. Untuk multi-room dinamis, buat router per roomId.
    router = await worker.createRouter({ mediaCodecs });
}

createWorker();

// Simpan state transport, producer, dan consumer
const transports = new Map();
const producers = new Map();
const consumers = new Map();

io.on('connection', (socket) => {
    console.log(`User terhubung: ${socket.id}`);

    // Mengirim RTP Capabilities ke klien untuk inisialisasi Device
    socket.on('getRouterRtpCapabilities', (callback) => {
        callback(router.rtpCapabilities);
    });

    // Membuat WebRTC Transport di sisi server (untuk Send atau Receive)
    socket.on('createWebRtcTransport', async (_, callback) => {
        try {
            const transport = await router.createWebRtcTransport({
                listenIps: [{ ip: '0.0.0.0', announcedIp: '192.168.111.181' }], // Ganti announcedIp dengan IP Publik/Domain Anda
                // listenIps: [{ ip: '0.0.0.0', announcedIp: '127.0.0.1' }], // Ganti announcedIp dengan IP Publik/Domain Anda
                enableUdp: true,
                enableTcp: true,
                preferUdp: true,
            });

            transports.set(transport.id, transport);

            transport.on('dtlsstatechange', dtlsState => {
                if (dtlsState === 'closed') transport.close();
            });

            transport.on('close', () => console.log('Transport ditutup'));

            callback({
                params: {
                    id: transport.id,
                    iceParameters: transport.iceParameters,
                    iceCandidates: transport.iceCandidates,
                    dtlsParameters: transport.dtlsParameters
                }
            });
        } catch (error) {
            callback({ error: error.message });
        }
    });

    // Menghubungkan transport (DTLS handshake)
    socket.on('transport-connect', async ({ dtlsParameters, transportId }) => {
        const transport = transports.get(transportId);
        await transport.connect({ dtlsParameters });
    });

    // Memulai stream (Penyiar)
    socket.on('transport-produce', async ({ kind, rtpParameters, transportId }, callback) => {
        const transport = transports.get(transportId);
        const producer = await transport.produce({ kind, rtpParameters });

        producers.set(producer.id, producer);

        producer.on('transportclose', () => {
            producer.close();
        });

        // Beritahu klien lain ada track baru
        socket.broadcast.emit('new-producer', producer.id);

        callback({ id: producer.id });
    });

    // Menerima stream (Penonton)
    socket.on('consume', async ({ rtpCapabilities, transportId, producerId }, callback) => {
        try {
            if (!router.canConsume({ producerId, rtpCapabilities })) {
                console.error('Klien tidak dapat mengonsumsi stream ini');
                return;
            }

            const transport = transports.get(transportId);
            const consumer = await transport.consume({
                producerId,
                rtpCapabilities,
                paused: true // Dimulai dalam keadaan paused, klien harus mengirim sinyal resume
            });

            consumers.set(consumer.id, consumer);

            consumer.on('transportclose', () => {
                consumer.close();
            });

            callback({
                params: {
                    id: consumer.id,
                    producerId: consumer.producerId,
                    kind: consumer.kind,
                    rtpParameters: consumer.rtpParameters
                }
            });
        } catch (error) {
            callback({ error: error.message });
        }
    });

    socket.on('consumer-resume', async ({ consumerId }) => {
        const consumer = consumers.get(consumerId);
        await consumer.resume();
    });

    socket.on('disconnect', () => {
        console.log(`User terputus: ${socket.id}`);
        // TODO: Bersihkan transport, producer, dan consumer milik user ini
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Signaling SFU berjalan di http://localhost:${PORT}`);
});