package beam_dataflow.DataflowReader;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import beam_dataflow.DataflowUtil.YamlUtil;

public class XLSXFileReader implements Serializable {

	private YamlUtil feeds;
	private String feed_id;
	String timestamp=Instant.now().toString();
	private static final long serialVersionUID = 1L;

	public XLSXFileReader(YamlUtil feed, String feed_ids) throws IOException {
		feeds = feed;
		feed_id = feed_ids;

	}

	@SuppressWarnings({ "unchecked", "unused" })
	public List<JSONObject> read(XSSFWorkbook wb, String file) throws ParseException, IOException {

		String xl_column_row = feeds.getfeeds(feed_id, "dataflow_xl_column_row");
		String xl_header_col = feeds.getfeeds(feed_id, "dataflow_xl_header_col");
		String xl_row_num = feeds.getfeeds(feed_id, "dataflow_xl_row_num");
		String xlsx_sheet_pattern = feeds.getfeeds(feed_id, "dataflow_xl_sheet_pattern");

		List<JSONObject> json_list = new ArrayList<JSONObject>();
		List<String> c = new ArrayList<String>();
		List<String> sheet_array = new ArrayList<String>();

		if (xl_header_col.contains(",")) {
			String[] items = xl_header_col.split(",");
			c = Arrays.asList(items);
		} else {
			String[] temp = xl_header_col.split(":");
			for (int b = Integer.parseInt(temp[0]); b <= Integer.parseInt(temp[1]); b++) {
				c.add(String.valueOf(b));
			}
		}
		List<String> r = new ArrayList<String>();
		if (xl_row_num.contains(":")) {
			String[] items = xl_row_num.split(":");
			r = Arrays.asList(items);
		} else {
			r.add(xl_row_num);
		}

		if (xlsx_sheet_pattern.contains(",")) {
			String[] items = xlsx_sheet_pattern.split(",");
			sheet_array = Arrays.asList(items);
		} else {
			sheet_array.add(xlsx_sheet_pattern);
		}

		Integer[] arr = new Integer[c.size()];
		for (int a = 0; a < c.size(); a++) {
			arr[a] = Integer.parseInt(c.get(a));
		}
		for (int index = 0; index < wb.getNumberOfSheets(); index++) {
			List<String> column = new ArrayList<String>();
			XSSFSheet sheet = wb.getSheetAt(index);
			int flag = 0;
			if(!xlsx_sheet_pattern.equals("false")) {
			for (int sh = 0; sh < sheet_array.size(); sh++) {
				Pattern pattern = Pattern.compile(sheet_array.get(sh));
				Matcher matcher = pattern.matcher(sheet.getSheetName().toString());
				if (matcher.find())
					flag = 1;
			}}else {flag = 1;}
			if (flag == 1) {
				Iterator<Row> itr = sheet.iterator();
				List<String> column1 = new ArrayList<String>();
				Row row = sheet.getRow(Integer.parseInt(xl_column_row) - 1);
				int dcol = 1;
				int s = 0;
				for (int xl_col = Integer.parseInt(c.get(0)) - 1; xl_col <= Integer.parseInt(c.get(c.size() - 1))
						- 1; xl_col++) {
					if (arr[s] == xl_col + 1) {

						if (row.getCell(xl_col) == null) {
							column.add("missing_column_" + dcol + 1);
							dcol++;
						} else {
							column.add(row.getCell(xl_col).toString());
						}
					} else {
						continue;
					}
					s++;
				}

				Row data_row = null;
				int counter = 1;

				Iterator<Row> itr_row = sheet.iterator();

				while (itr_row.hasNext()) {
					JSONObject xlsx_jo = new JSONObject();
					JSONObject success_error = new JSONObject();
					JSONObject error_rec = new JSONObject();
					data_row = itr_row.next();
					int row_start = Integer.parseInt(r.get(0)) - 1;

					if (data_row.getRowNum() >= row_start) {
						if (xl_row_num.contains(":")) {
							if (data_row.getRowNum() == Integer.parseInt(r.get(1))) {
								break;
							}
						}
						try {
							if (data_row.getPhysicalNumberOfCells() <= arr.length) {
								int rcol = 1;
								int s1 = 0;
								for (int xl_col = Integer.parseInt(c.get(0)) - 1; xl_col <= Integer
										.parseInt(c.get(c.size() - 1)) - 1; xl_col++) {
									if (arr[s1] == xl_col + 1) {
										if (data_row.getCell(xl_col) == null) {
											xlsx_jo.put(column.get(s1), null);
											rcol++;
										} else {

											if (data_row.getCell(xl_col).getCellType() == CellType.NUMERIC) {
												double data = data_row.getCell(xl_col).getNumericCellValue();
												if (data % 1 == 0) {
													int value = (int) data;
													xlsx_jo.put(column.get(s1), String.valueOf(value));
												} else {
													xlsx_jo.put(column.get(s1), data_row.getCell(xl_col).toString());
												}
											} else {
												xlsx_jo.put(column.get(s1), data_row.getCell(xl_col).toString());
											}
										}
										xlsx_jo.put("insert_timestamp", timestamp);
										xlsx_jo.put("sys_information_dict", "{'file_name':'" + file + "','sheet_name':'"
												+ sheet.getSheetName() + "'}");
									} else {
										continue;
									}

									s1++;
								}
							} else {
								xlsx_jo.put("error", "");
								List<String> error_record = new ArrayList<String>();
								for (int xl_col = Integer.parseInt(c.get(0)) - 1; xl_col <= data_row
										.getPhysicalNumberOfCells(); xl_col++) {
									error_record.add(data_row.getCell(xl_col).toString());
								}
								error_rec.put("row_number", String.valueOf(data_row.getRowNum() + 1));
								error_rec.put("insert_timestamp", timestamp);
								error_rec.put("error_type", " Column Mismatch");
								error_rec.put("payload", error_record.toString());
								error_rec.put("sys_information_dict",
										"{'file_name':'" + file + "','sheet_name':'" + sheet.getSheetName() + "'}");
							}
						} catch (Exception e) {
							xlsx_jo.put("error", "");
							List<String> error_record = new ArrayList<String>();
							for (int xl_col = Integer.parseInt(c.get(0)) - 1; xl_col <= data_row
									.getPhysicalNumberOfCells(); xl_col++) {
								error_record.add(data_row.getCell(xl_col).toString());
							}
							error_rec.put("row_number", String.valueOf(data_row.getRowNum() + 1));
							error_rec.put("insert_timestamp", timestamp);
							error_rec.put("error_type", e.toString());
							error_rec.put("payload", error_record.toString());
							error_rec.put("sys_information_dict",
									"{'file_name':'" + file + "','sheet_name':'" + sheet.getSheetName() + "'}");
						}

						success_error.put("success", xlsx_jo.toString());
						success_error.put("failure", error_rec.toString());
						json_list.add(success_error);
					}
				}
			}
		}
		return json_list;
	}

	@SuppressWarnings("unchecked")
	public List<JSONObject> melt(XSSFWorkbook wb, String file) throws ParseException, IOException {

		String xlsx_sheet_pattern = feeds.getfeeds(feed_id, "dataflow_xl_sheet_pattern");
		String xl_column_row = feeds.getfeeds(feed_id, "dataflow_xl_column_row");
		String xl_header_col = feeds.getfeeds(feed_id, "dataflow_xl_header_col");
		String xl_row_num = feeds.getfeeds(feed_id, "dataflow_xl_row_num");
		String xl_melt_col = feeds.getfeeds(feed_id, "dataflow_xl_melt_col");
		String xl_fixed_col = feeds.getfeeds(feed_id, "dataflow_xl_fixed_col");

		List<JSONObject> json_list = new ArrayList<JSONObject>();
		List<String> sheet_array = new ArrayList<String>();

		String[] temp_sheet = xlsx_sheet_pattern.split(",");
		sheet_array = Arrays.asList(temp_sheet);

		// setting row of header as 2
		List<String> c = new ArrayList<String>();
		List<String> r = new ArrayList<String>();
		List<String> xl_col = new ArrayList<String>();
		List<String> melt_c = new ArrayList<String>();

		if (xl_row_num.contains(":")) {
			String[] items = xl_row_num.split(":");
			r = Arrays.asList(items);
		} else {
			r.add(xl_row_num);
		}
		if (xl_header_col.contains(",")) {
			String[] items = xl_header_col.split(",");
			xl_col = Arrays.asList(items);
		} else {
			String[] temp = xl_header_col.split(":");

			for (int b = Integer.parseInt(temp[0]); b <= Integer.parseInt(temp[1]); b++) {
				xl_col.add(String.valueOf(b));
			}
		}

		Integer[] arr_col = new Integer[xl_col.size()];
		for (int a = 0; a < xl_col.size(); a++) {
			arr_col[a] = Integer.parseInt(xl_col.get(a));
		}

		if (xl_melt_col.contains(",")) {
			String[] items = xl_melt_col.split(",");
			c = Arrays.asList(items);
		} else {
			String[] temp = xl_melt_col.split(":");

			for (int b = Integer.parseInt(temp[0]); b <= Integer.parseInt(temp[1]); b++) {
				c.add(String.valueOf(b));
			}
		}

		Integer[] arr = new Integer[c.size()];
		for (int a = 0; a < c.size(); a++) {
			arr[a] = Integer.parseInt(c.get(a));
		}

		// melt cols
		if (xl_fixed_col.contains(",")) {
			String[] items = xl_fixed_col.split(",");
			melt_c = Arrays.asList(items);
		} else {
			String[] temp = xl_fixed_col.split(":");

			for (int b = Integer.parseInt(temp[0]); b <= Integer.parseInt(temp[1]); b++) {
				melt_c.add(String.valueOf(b));
			}
		}

		Integer[] melt_arr = new Integer[melt_c.size()];
		for (int a = 0; a < melt_c.size(); a++) {
			melt_arr[a] = Integer.parseInt(melt_c.get(a));
		}

		for (int index = 0; index < wb.getNumberOfSheets(); index++) {

			XSSFSheet sheet = wb.getSheetAt(index);
			int flag = 0;
			for (int sh = 0; sh < sheet_array.size(); sh++) {
				Pattern pattern = Pattern.compile(sheet_array.get(sh));
				Matcher matcher = pattern.matcher(sheet.getSheetName().toString());
				if (matcher.find())
					flag = 1;
			}
			if (flag == 1) {
				List<String> column = new ArrayList<String>();
				Row row = sheet.getRow(Integer.parseInt(xl_column_row) - 1);
				int dcol = 1;
				int s = 0;
				for (int xl_c = Integer.parseInt(xl_col.get(0)) - 1; xl_c <= Integer
						.parseInt(xl_col.get(xl_col.size() - 1)) - 1; xl_c++) {
					if (arr_col[s] == xl_c + 1) {

						if (row.getCell(xl_c) == null || row.getCell(xl_c).getCellType() == CellType.BLANK) {
							column.add("missing_column_" + dcol);
							dcol++;
						} else {
							column.add(row.getCell(xl_c).toString());
						}

					} else {
						continue;
					}
					s++;
				}

				for (int i = Integer.parseInt(xl_column_row); i < sheet.getPhysicalNumberOfRows(); i++) {
					if (xl_row_num.contains(":")) {
						if (i == Integer.parseInt(r.get(1))) {
							break;
						}
					}

					Row dataRow = sheet.getRow(i);
					try {
						for (int dc = arr[0] - 1; dc < column.size(); dc++) { // col as row
							JSONObject success_error = new JSONObject();
							JSONObject jo = new JSONObject();
							for (int j = 0; j < melt_arr.length; j++) {
								if (dataRow.getCell(melt_arr[j] - 1).getCellType() == CellType.NUMERIC) {
									double data = dataRow.getCell(melt_arr[j] - 1).getNumericCellValue();
									if (data % 1 == 0) {
										int value = (int) data;
										jo.put(column.get(j), value);
									} else {
										jo.put(column.get(j), dataRow.getCell(melt_arr[j] - 1).toString());
									}
								} else {
									jo.put(column.get(j), dataRow.getCell(melt_arr[j] - 1).toString());
								}

								if ((dataRow.getCell(dc) == null)
										|| (dataRow.getCell(dc).getCellType() == CellType.BLANK)) {
									jo.put("key", column.get(dc));
									jo.put("value", null);
								} else {
									if (dataRow.getCell(dc).getCellType() == CellType.NUMERIC) {
										double data = dataRow.getCell(dc).getNumericCellValue();
										if (data % 1 == 0) {
											int value = (int) data;
											jo.put("key", column.get(dc));
											jo.put("value", String.valueOf(value));
										} else {
											jo.put("key", column.get(dc));
											jo.put("value", dataRow.getCell(dc).toString());
										}
									} else {
										jo.put("key", column.get(dc));
										jo.put("value", dataRow.getCell(dc).toString());
									}
								}

							}
							jo.put("insert_timestamp", timestamp);
							jo.put("sys_information_dict",
									"{'file_name':'" + file + "','sheet_name':'" + sheet.getSheetName() + "'}");
							success_error.put("success", jo.toJSONString());
							success_error.put("failure", "{}");
							json_list.add(success_error);

						}
					} catch (Exception e) {

					}

				}

			}
		}
		return json_list;
	}

}
