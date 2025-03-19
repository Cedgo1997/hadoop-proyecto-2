import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalisisPeliculas {

    // Mapper para el país con mayor rating por año
    public static class PaisMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text año = new Text();
        private Text paisRating = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] campos = parseCSVLine(value.toString()); // Usar el parser de CSV manual

            // Verificar que la línea tenga al menos 7 campos
            if (campos.length >= 7) {
                String añoValue = campos[2]; // Año (date)
                String pais = campos[4]; // País (country)
                String rating = campos[3]; // Rating

                // Verificar que los campos no estén vacíos
                if (!añoValue.isEmpty() && !pais.isEmpty() && !rating.isEmpty()) {
                    try {
                        // Convertir el rating a double para asegurarnos de que es un número válido
                        Double.parseDouble(rating);

                        // Emitir: clave = año, valor = (país, rating)
                        año.set(añoValue);
                        paisRating.set(pais + "," + rating);
                        context.write(año, paisRating);
                    } catch (NumberFormatException e) {
                        // Ignorar valores no numéricos en el rating
                        System.err.println("Valor no numérico en rating: " + rating);
                    }
                }
            } else {
                // Registrar una advertencia si la línea no tiene suficientes campos
                System.err.println("Línea ignorada: no tiene suficientes campos - " + value.toString());
            }
        }

        // Método para dividir una línea de CSV respetando comas dentro de comillas
        private String[] parseCSVLine(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder currentField = new StringBuilder();
            boolean insideQuotes = false;

            for (char c : line.toCharArray()) {
                if (c == '"') {
                    insideQuotes = !insideQuotes; // Cambiar el estado de "dentro de comillas"
                } else if (c == ',' && !insideQuotes) {
                    // Si no estamos dentro de comillas, agregar el campo a la lista
                    fields.add(currentField.toString());
                    currentField.setLength(0); // Reiniciar el campo actual
                } else {
                    currentField.append(c); // Agregar el carácter al campo actual
                }
            }

            // Agregar el último campo
            fields.add(currentField.toString());

            return fields.toArray(new String[0]);
        }
    }

    // Reducer para el país con mayor rating por año
    public static class PaisReducer extends Reducer<Text, Text, Text, Text> {
        private Text resultado = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Double> sumaRatings = new HashMap<>(); // Suma de ratings por país
            HashMap<String, Integer> conteoPaises = new HashMap<>(); // Conteo de películas por país

            // Calcular la suma de ratings y el conteo de películas por país
            for (Text value : values) {
                String[] partes = value.toString().split(",");
                String pais = partes[0];
                double rating = Double.parseDouble(partes[1]);

                sumaRatings.put(pais, sumaRatings.getOrDefault(pais, 0.0) + rating);
                conteoPaises.put(pais, conteoPaises.getOrDefault(pais, 0) + 1);
            }

            // Calcular el país con el mayor rating promedio
            String paisMax = "";
            double maxPromedio = -1;

            for (String pais : sumaRatings.keySet()) {
                double promedio = sumaRatings.get(pais) / conteoPaises.get(pais);
                if (promedio > maxPromedio) {
                    maxPromedio = promedio;
                    paisMax = pais;
                }
            }

            // Emitir: clave = año, valor = (país, rating promedio)
            resultado.set("|" + paisMax + "|" + maxPromedio);
            context.write(key, resultado);
        }
    }

    // Mapper para el tipo de película con mayor rating por año
    public static class TipoMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text año = new Text();
        private Text tipoRating = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] campos = parseCSVLine(value.toString()); // Usar el parser de CSV manual

            // Verificar que la línea tenga al menos 7 campos
            if (campos.length >= 7) {
                String añoValue = campos[2]; // Año (date)
                String tipo = campos[6]; // Tipo de película (theme)
                String rating = campos[3]; // Rating

                // Verificar que los campos no estén vacíos
                if (!añoValue.isEmpty() && !tipo.isEmpty() && !rating.isEmpty()) {
                    try {
                        // Convertir el rating a double para asegurarnos de que es un número válido
                        Double.parseDouble(rating);

                        // Emitir: clave = año, valor = (tipo, rating)
                        año.set(añoValue);
                        tipoRating.set(tipo + "," + rating);
                        context.write(año, tipoRating);
                    } catch (NumberFormatException e) {
                        // Ignorar valores no numéricos en el rating
                        System.err.println("Valor no numérico en rating: " + rating);
                    }
                }
            } else {
                // Registrar una advertencia si la línea no tiene suficientes campos
                System.err.println("Línea ignorada: no tiene suficientes campos - " + value.toString());
            }
        }

        // Método para dividir una línea de CSV respetando comas dentro de comillas
        private String[] parseCSVLine(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder currentField = new StringBuilder();
            boolean insideQuotes = false;

            for (char c : line.toCharArray()) {
                if (c == '"') {
                    insideQuotes = !insideQuotes; // Cambiar el estado de "dentro de comillas"
                } else if (c == ',' && !insideQuotes) {
                    // Si no estamos dentro de comillas, agregar el campo a la lista
                    fields.add(currentField.toString());
                    currentField.setLength(0); // Reiniciar el campo actual
                } else {
                    currentField.append(c); // Agregar el carácter al campo actual
                }
            }

            // Agregar el último campo
            fields.add(currentField.toString());

            return fields.toArray(new String[0]);
        }
    }

    // Reducer para el tipo de película con mayor rating por año
    public static class TipoReducer extends Reducer<Text, Text, Text, Text> {
        private Text resultado = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Double> sumaRatings = new HashMap<>(); // Suma de ratings por tipo
            HashMap<String, Integer> conteoTipos = new HashMap<>(); // Conteo de películas por tipo

            // Calcular la suma de ratings y el conteo de películas por tipo
            for (Text value : values) {
                String[] partes = value.toString().split(",");
                String tipo = partes[0];
                double rating = Double.parseDouble(partes[1]);

                sumaRatings.put(tipo, sumaRatings.getOrDefault(tipo, 0.0) + rating);
                conteoTipos.put(tipo, conteoTipos.getOrDefault(tipo, 0) + 1);
            }

            // Calcular el tipo con el mayor rating promedio
            String tipoMax = "";
            double maxPromedio = -1;

            for (String tipo : sumaRatings.keySet()) {
                double promedio = sumaRatings.get(tipo) / conteoTipos.get(tipo);
                if (promedio > maxPromedio) {
                    maxPromedio = promedio;
                    tipoMax = tipo;
                }
            }

            // Emitir: clave = año, valor = (tipo, rating promedio)
            resultado.set("|" + tipoMax + "|" + maxPromedio);
            context.write(key, resultado);
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        // Configuración del Job 1: País con mayor rating por año
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "País con mayor rating por año");
        job1.setJarByClass(AnalisisPeliculas.class);
        job1.setMapperClass(PaisMapper.class);
        job1.setReducerClass(PaisReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0])); // Ruta de entrada
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); // Ruta de salida para Job 1
        job1.waitForCompletion(true);

        // Configuración del Job 2: Tipo de película con mayor rating por año
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Tipo de película con mayor rating por año");
        job2.setJarByClass(AnalisisPeliculas.class);
        job2.setMapperClass(TipoMapper.class);
        job2.setReducerClass(TipoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0])); // Ruta de entrada
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Ruta de salida para Job 2
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
