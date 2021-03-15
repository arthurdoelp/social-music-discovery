const Sequelize = require("sequelize");
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
    console.log("PostgreSQL Connected!");

    db.sequelize = sequelize;
    db.Sequelize = Sequelize;

module.exports = db;