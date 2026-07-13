const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path'); // Tambahkan modul path

const app = express();
const server = http.createServer(app);

// Konfigurasi Socket.io
// CORS diizinkan dari semua origin untuk keperluan development
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    path: '/live/socket.io'
});

// Menyajikan file statis (client) jika index.html diletakkan di folder 'public'
app.use(express.static('public'));

app.get('/live/:roomId', (req, res) => {
    // Kita tetap mengirimkan file index.html yang sama,
    // nanti Javascript di client yang akan membaca ID dari URL
    res.sendFile(path.join(__dirname,  './public/live.html'));
});

app.get('/live', (req, res) => {
    // Kita tetap mengirimkan file index.html yang sama,
    // nanti Javascript di client yang akan membaca ID dari URL
    res.sendFile(path.join(__dirname,  './public/index.html'));
});
app.get('/private', (req, res) => {
    // Kita tetap mengirimkan file index.html yang sama,
    // nanti Javascript di client yang akan membaca ID dari URL
    res.sendFile(path.join(__dirname, './public/call.html'));
});

io.on('connection', (socket) => {
    console.log(`User terhubung: ${socket.id}`);

    // Saat penyiar atau penonton bergabung ke sebuah room (berdasarkan Room ID)
    socket.on('join-room', (roomId) => {
        socket.join(roomId);
        console.log(`${socket.id} bergabung ke ruangan: ${roomId}`);

        // Memberitahu semua orang di room tersebut (kecuali pengirim) bahwa ada penonton baru
        // Ini memicu penyiar untuk membuat 'Offer'
        socket.to(roomId).emit('viewer-joined');
    });

    // Meneruskan Offer (SDP) dari Penyiar ke Penonton
    socket.on('offer', (data) => {
        const { roomId, offer } = data;
        socket.to(roomId).emit('offer', offer);
    });

    // Meneruskan Answer (SDP) dari Penonton kembali ke Penyiar
    socket.on('answer', (data) => {
        const { roomId, answer } = data;
        socket.to(roomId).emit('answer', answer);
    });

    // Meneruskan ICE Candidates untuk mencari jalur P2P terbaik
    socket.on('ice-candidate', (data) => {
        const { roomId, candidate } = data;
        socket.to(roomId).emit('ice-candidate', candidate);
    });

    // Menangani saat user terputus
    socket.on('disconnect', () => {
        console.log(`User terputus: ${socket.id}`);
        // Anda bisa menambahkan logika tambahan di sini, 
        // misalnya memberitahu room bahwa penyiar offline
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Signaling server berjalan di http://localhost:${PORT}`);
    console.log(`Pastikan Anda meletakkan index.html di folder 'public' (jika ingin diakses langsung)`);
});