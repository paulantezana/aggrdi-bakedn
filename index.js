// app.js
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();

const OlympicWinnersService = require('./olympicWinnersService'); // Importar la función
 

// Middleware
app.use(cors()); // Habilitar CORS para todas las rutas
app.use(bodyParser.json()); // Para parsear solicitudes JSON
app.use(bodyParser.urlencoded({ extended: true })); // Para parsear solicitudes URL-encoded

// Ruta principal
app.get('/', (req, res) => {
    res.send('<h1>¡Hola, mundo desde Node.js con Nodemon, Body-Parser y CORS!</h1>');
});

// Ruta para manejar POST requests
app.post('/datos', (req, res) => {
    const datos = req.body;
    res.json({
        mensaje: "Datos recibidos correctamente",
        datos: datos
    });
});

app.post('/olympicWinners', function (req, res) {
    const pivot = new OlympicWinnersService();
    pivot.getData(req.body, (rows, lastRow, pivotFields) => {
        res.json({rows: rows, lastRow: lastRow, pivotFields});
    });
});

// Configurar el puerto de la aplicación
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
    console.log(`Servidor corriendo en el puerto ${PORT}`);
});
