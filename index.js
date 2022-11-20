const fs = require("fs");
const { createClient } = require("@supabase/supabase-js");
const { parse } = require("csv-parse");
const dotenv = require("dotenv");

dotenv.config();

// Data Warehouse
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// aplicacion en la que se efectuan las vacunaciones por plataforma
const supabaseApp = createClient(
  process.env.SUPABASE_APP_URL,
  process.env.SUPABASE_APP_KEY
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

const getDepartamentos = async () => {
  const { data: dataJurisdiccion, error } = await supabaseApp
    .from("jurisdicciones")
    .select("*");

  const departamentos = await Promise.all(
    dataJurisdiccion.map(async (jurisdiccion) => {
      const { data: departamentos } = await supabaseApp
        .from("departamentos")
        .select("*")
        .eq("jurisdiccion_id", jurisdiccion.id);
      return {
        jurisdiccion: jurisdiccion.nombre,
        departamentos:
          departamentos.length > 0
            ? departamentos.map((depto) => depto.nombre)
            : ["Departamento generado"],
      };
    })
  );

  return departamentos;

  //   if (!departamentos) return "Departamento generado automaticamente";

  //   // devolvemos un departamento random
  //   const randomI = Math.floor(Math.random() * departamentos.length);

  //   return departamentos[randomI].nombre;
};

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
      const departamentos = await getDepartamentos();

      const arrDLugar = records.map((row) => {
        const departamentosJurisdiccion = departamentos.find(
          (depto) => depto.jurisdiccion == row[2]
        ).departamentos;
        return {
          jurisdiccion: row[2],
          departamento:
            departamentosJurisdiccion[
              Math.floor(Math.random() * departamentosJurisdiccion.length)
            ],
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
