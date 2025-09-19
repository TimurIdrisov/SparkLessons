package s2t_merging;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ExcelMerger {

    public static void main(String[] args) throws IOException {
        String templatePath = "C:\\Users\\Тимур\\IdeaProjects\\SparkColumns\\src\\main\\resources\\s2t\\00_templ.xlsx";
        String resultPath = "result.xlsx";
        String folderWithData = "C:\\Users\\Тимур\\IdeaProjects\\SparkColumns\\src\\main\\resources\\s2t\\custvoice";

        try (FileInputStream templateStream = new FileInputStream(templatePath);
             Workbook templateWorkbook = new XSSFWorkbook(templateStream);
             Workbook resultWorkbook = new XSSFWorkbook()) {

            // Копируем все листы из шаблона в новый файл
            for (int i = 0; i < templateWorkbook.getNumberOfSheets(); i++) {
                Sheet originalSheet = templateWorkbook.getSheetAt(i);
                Sheet newSheet = resultWorkbook.createSheet(originalSheet.getSheetName());
                copySheet(originalSheet, newSheet);
            }

            // Обновляем содержимое листов из файлов в папке s2t
            List<String> sheetsToUpdate = List.of("s2t", "Target", "source");

            for (String sheetName : sheetsToUpdate) {
                Path filePath = Paths.get(folderWithData, sheetName + ".xlsx");
                if (Files.exists(filePath)) {
                    try (Workbook dataWorkbook = new XSSFWorkbook(Files.newInputStream(filePath))) {
                        Sheet dataSheet = dataWorkbook.getSheetAt(0); // Берем первый лист
                        Sheet resultSheet = resultWorkbook.getSheet(sheetName);

                        if (resultSheet != null) {
                            // Очищаем старые строки
                            int lastRow = resultSheet.getLastRowNum();
                            for (int i = lastRow; i >= 0; i--) {
                                Row row = resultSheet.getRow(i);
                                if (row != null) resultSheet.removeRow(row);
                            }

                            // Копируем с 3-й строки исходного файла
                            copySheetFromRow(dataSheet, resultSheet, 2, 0);
                        }
                    }
                } else {
                    System.out.println("Файл не найден: " + filePath);
                }
            }

            // Сохраняем итоговый файл
            try (FileOutputStream out = new FileOutputStream(resultPath)) {
                resultWorkbook.write(out);
                System.out.println("Файл result.xlsx создан успешно!");
            }
        }
    }
    private static void copySheetFromRow(Sheet src, Sheet dest, int startRowSrc, int startRowDest) {
        int destRowNum = startRowDest;

        for (int i = startRowSrc; i <= src.getLastRowNum(); i++) {
            Row srcRow = src.getRow(i);
            if (srcRow == null) continue;

            Row destRow = dest.createRow(destRowNum++);

            for (int j = 0; j < srcRow.getLastCellNum(); j++) {
                Cell srcCell = srcRow.getCell(j);
                if (srcCell == null) continue;

                Cell destCell = destRow.createCell(j);
                copyCellValue(srcCell, destCell);
            }
        }
    }

    // Метод для копирования данных с одного листа на другой
    private static void copySheet(Sheet src, Sheet dest) {
        for (int i = 0; i <= src.getLastRowNum(); i++) {
            Row srcRow = src.getRow(i);
            if (srcRow == null) continue;

            Row destRow = dest.createRow(i);

            for (int j = 0; j < srcRow.getLastCellNum(); j++) {
                Cell srcCell = srcRow.getCell(j);
                if (srcCell == null) continue;

                Cell destCell = destRow.createCell(j);
                copyCellValue(srcCell, destCell);
            }
        }
    }

    // Метод для копирования значений ячеек
    private static void copyCellValue(Cell src, Cell dest) {
        switch (src.getCellType()) {
            case STRING:
                dest.setCellValue(src.getStringCellValue());
                break;
            case NUMERIC:
                dest.setCellValue(src.getNumericCellValue());
                break;
            case BOOLEAN:
                dest.setCellValue(src.getBooleanCellValue());
                break;
            case FORMULA:
                dest.setCellFormula(src.getCellFormula());
                break;
            case BLANK:
                dest.setBlank();
                break;
            default:
                dest.setCellValue(src.toString());
                break;
        }
    }
}
