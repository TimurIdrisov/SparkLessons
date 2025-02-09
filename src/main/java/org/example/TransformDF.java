package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TransformDF {

    private Dataset<Row> df;

    public TransformDF(Dataset<Row> df){
        this.df = df;

    }
    public Dataset<Row> getDf(){
        return df;
    }
//метод разворачивает колонки DF кроме статичных колонок staticColumns,
    public void unpivotDf (List<String> staticColumns) {
        String[] columnsToUnpivot = Arrays.stream(df.columns())
                .filter(col -> !staticColumns.contains(col))
                .toArray(String[]::new);

        // Выполнение unpivot через динамический stack
        String stackExpr = Arrays.stream(columnsToUnpivot)
                .map(col -> "'" + col + "', " + col)
                .collect(Collectors.joining(", "));

        String stackQuery = String.format("stack(%d, %s) as (ques_id, ans)", columnsToUnpivot.length, stackExpr);

        staticColumns.add(stackQuery);

        String[] selectExpression = staticColumns.toArray(new String[0]);

        df = df.selectExpr(selectExpression)
                .filter("Ans is not NULL");

        // Результат
        System.out.println("Результат после динамического melt:");
//        df.show();
    }

//Метод переименовывает колонки и удаляет лишние колонки
    public void renameColumns(HashMap<String, String> columnsToRename, List<String> staticColumns) {
        // Собираем в коллекцию колонки, у которых значение null в HashMap
        List<String> columnsToDrop = columnsToRename.entrySet().stream()
                .filter(entry -> entry.getValue() == null)
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        // Убираем в исходном df колонки к которым нет названия
        if (!columnsToDrop.isEmpty()) {
            df = df.drop(columnsToDrop.toArray(new String[0]));
        }

        // Переименовываем оставшиеся колонки
        for (String col : columnsToRename.keySet()) {
            if (columnsToRename.get(col) != null && !staticColumns.contains(col)) {
                df = df.withColumnRenamed(col, columnsToRename.get(col));
            }
        }

        // Выводим финальный результат
//        df.show();
    }
}
