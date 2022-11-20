const fs = require("fs");
const { createClient } = require("@supabase/supabase-js");
const { parse } = require("csv-parse");
const dotenv = require("dotenv");

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const uploadRows = (table, rows) => {
  return supabase.from(table).insert(rows).select();
};

const clearDataWarehouse = async () => {
  await supabase.from("h_vacunados").delete().neq("id", 0);
  await supabase.from("d_lugar").delete().neq("id", 0);
  await supabase.from("d_tiempo").delete().neq("id", 0);
  await supabase.from("d_vacuna").delete().neq("id", 0);
  await supabase.from("d_vacunado").delete().neq("id", 0);
};

// [
//   "anticovid_pfizer",
//   "666",
//   "CABA",
//   "2021-09-25",
//   "30000095",
//   "1989-04-19",
//   "F",
// ];

const loadDataWarehouse = async () => {
  const records = [];

  fs.createReadStream("./1m.csv")
    .pipe(parse({ delimiter: ",", from_line: 2 }))
    .on("data", function (row) {
      records.push(row);
    })
    .on("end", async function () {
      console.log("|| Termine de cargar el CSV ");

      // adaptamos el formato a nuestra DB
      const arrDLugar = records.map((row) => {
        return {
          jurisdiccion: row[2],
          departamento: 1, // AGregar un numero random
        };
      });
      const arrDTiempo = records.map((row) => {
        return {
          año: row[3].split("-")[0],
          mes: row[3].split("-")[1],
          dia: row[3].split("-")[2],
        };
      });
      const arrDVacuna = records.map((row) => {
        return {
          nombre: row[0].split("_")[0],
          laboratorio: row[0].split("_")[1],
          tipo_vacuna: row[0].split("_")[1], // Agregar el tipo de vacuna
        };
      });
      const arrDVacunado = records.map((row) => {
        const edad = Math.floor(
          (new Date().getTime() - new Date(row[5]).getTime()) /
            (1000 * 60 * 60 * 24 * 365)
        );
        return {
          dni: row[4],
          anio: edad,
          decenio: Math.floor(edad / 10),
          bicenio: Math.floor(edad / 20),
        };
      });

      // cargamos de a 10000 filas en 10000 filas
      //   const pages = Math.floor(records.length / 10000);
      const pages = 1;

      for (let i = 0; i < pages; i++) {
        const { data: dataTiempo } = await uploadRows(
          "d_tiempo",
          arrDTiempo.slice(i * 10000, i * 10000 + 10000)
        );
        const { data: dataLugar } = await uploadRows(
          "d_lugar",
          arrDLugar.slice(i * 10000, i * 10000 + 10000)
        );
        const { data: dataVacuna } = await uploadRows(
          "d_vacuna",
          arrDVacuna.slice(i * 10000, i * 10000 + 10000)
        );
        const { data: dataVacunado, error } = await uploadRows(
          "d_vacunado",
          arrDVacunado.slice(i * 10000, i * 10000 + 10000)
        );

        console.log(error);

        const arrHechos = dataTiempo.map((tiempo, i) => {
          return {
            id_tiempo: tiempo.id,
            id_lugar: dataLugar[i].id,
            id_vacuna: dataVacuna[i].id,
            id_vacunado: dataVacunado[i].id,
          };
        });

        await uploadRows("h_vacunados", arrHechos);

        console.log(
          `|| cargados ${i * 10000 + 10000} de ${records.length} registros`
        );
      }
    })
    .on("error", function (error) {
      console.log(error.message);
    });
};

if (process.argv[2] == "clear") clearDataWarehouse();
if (process.argv[2] == "load") loadDataWarehouse();
