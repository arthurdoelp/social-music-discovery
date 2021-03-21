const Sequelize = require("sequelize");
if (process.env.GOOGLE_HOST) {
    const db = {};
    const sequelize = new Sequelize(process.env.GOOGLE_DATABASE, process.env.GOOGLE_USER, process.env.GOOGLE_PASS, {
        host: process.env.GOOGLE_HOST,
        dialect: 'postgres',

        pool: {
            max: 5,
            min: 0,
            acquire: 30000,
            idle: 10000
        }

    });
    console.log("Google Cloud PostgreSQL Connected!");

    db.sequelize = sequelize;
    db.Sequelize = Sequelize;

    module.exports = db;
} else {
    const db = {};
    const sequelize = new Sequelize("social-music-discovery", "arthurdoelp", "", {
        host: 'localhost',
        dialect: 'postgres',

        pool: {
            max: 5,
            min: 0,
            acquire: 30000,
            idle: 10000
        }

    });
    console.log("Localhost PostgreSQL Connected!");

    db.sequelize = sequelize;
    db.Sequelize = Sequelize;

    module.exports = db;
}

