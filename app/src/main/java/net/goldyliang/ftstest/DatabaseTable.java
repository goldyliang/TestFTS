package net.goldyliang.ftstest;

import android.content.ContentValues;
import android.content.Context;
import android.content.CursorLoader;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.os.AsyncTask;
import android.provider.BaseColumns;
import android.text.Html;
import android.util.Log;
import android.database.Cursor;
import android.widget.SimpleCursorAdapter;
import android.widget.TextView;

import java.io.File;
import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by gordon on 10/21/15.
 */
public class DatabaseTable {

    private static final String TAG = "FTSDatabase";

    //The columns we'll include in the dictionary table
    public static final String COL_LOCATION = "LOCATION";
    public static final String COL_TEXT = "TEXT";
    public static final String COL_DOCID = "docid";
    public static final String COL_BOOKTITLE = "TITLE";


    private static final String COL_FILEID = "ID";
    private static final String COL_FILEPATH = "PATH";
    private static final String COL_FILESIZE = "SIZE";
    private static final String COL_MODIFIED_TIME = "MODIFIED";
    private static final String COL_INDEXPOS = "INDEXPOS";

    private static final String COL_ROWID = "rowid";

    private static final String DATABASE_NAME = "EGW";
    private static final String FTS_VIRTUAL_TABLE = "FTS";
    private static final String FTS_CONTENT_TABLE = "CONTENT_TABLE";
    private static final String FILE_STATUS_TABLE = "FILE_STATUS";

    private static final String FTS_TABLE_CREATE =
            "CREATE VIRTUAL TABLE " + FTS_VIRTUAL_TABLE +
                    " USING fts4 (" +
                    "content=" + FTS_CONTENT_TABLE + ", " +
                    //"content=\"\", " +
                    COL_FILEID + ", " +
                    COL_FILEPATH + ", " +
                    COL_LOCATION + ", " +
                    COL_TEXT +
                    //", tokenize=icu zh_CN)";
                    ")";

    private static final String FTS_CONTENT_TABLE_CREATE =
            "CREATE TABLE " + FTS_CONTENT_TABLE
                    + " ( " + COL_DOCID +  " INTEGER PRIMARY KEY, "
                    + COL_FILEID + ", "
                    + COL_FILEPATH + ", "
                    + COL_LOCATION + ", "
                    + COL_TEXT
                    +")";



    private static final String FILE_STATUS_TABLE_CREATE =
            "CREATE TABLE " + FILE_STATUS_TABLE
                    + " ( " //+ COL_FILEID + ","
                    + COL_FILEPATH + ","
                    + COL_FILESIZE + ","
                    + COL_MODIFIED_TIME + ","
                    + COL_INDEXPOS + " ) ";

    private static final int DATABASE_VERSION = 1;

    private final DatabaseOpenHelper mDatabaseOpenHelper;

    private static final String FORMAT_HIGHTLIGHT_START = "<font color='red'>";
    private static final String FORMAT_HIGHTLIGHT_END = "</font>";
    private static final String DATABASE_FILE = "index.db";

    private BreakIterator mBreakIter;// = BreakIterator.getWordInstance(Locale.CHINA);
   /*
        mBreakIter = BreakIterator.getWordInstance(Locale.CHINESE);
    } */

    private String mDir;

    public static class BackgroundCreateIndexTask extends AsyncTask<DatabaseTable, Float, Boolean> {

        protected Boolean doInBackground(DatabaseTable... db) {

            return db[0].loadDatabase(this);
        }

        public void updateProgress (Float... values) {
            this.publishProgress(values);
        }
    }

    private BackgroundCreateIndexTask mAsyncTask = null;

    public DatabaseTable(String dir, BreakIterator iter) {

        mDir = dir;
        mBreakIter = iter;

        mDatabaseOpenHelper = new DatabaseOpenHelper(dir, DATABASE_FILE, null, 1);
    }

    public synchronized String getTokenizedText (String txt, List<String> words) {
        mBreakIter.setText(txt);
        int start = mBreakIter.first();
        int end = mBreakIter.next();

        StringBuffer buf = new StringBuffer();
        if (words!=null) words.clear();

        while (end != BreakIterator.DONE) {
            String word = txt.substring(start,end).trim();
            if (!word.isEmpty()) {
                if (words!=null) words.add(word);
                buf.append(txt.substring(start, end));
                buf.append(" ");
            }
            start = end;
            end = mBreakIter.next();
        }

        buf.deleteCharAt(buf.length()-1);

        return buf.toString();
    }

    private static String getHighlightedText (String txt, List<String> words) {
        StringBuffer buf = new StringBuffer(txt);

        for (String word: words) {
            int i = 0;
            while ((i = buf.indexOf(word, i)) >= 0) {
                String formated_word = FORMAT_HIGHTLIGHT_START + word + FORMAT_HIGHTLIGHT_END;
                buf.replace(i, i + word.length(), formated_word);
                i = i + formated_word.length();
            }
        }

        return new String (buf);
    }





    private class DatabaseOpenHelper extends SDCardSQLiteOpenHelper {

        //private final Context mHelperContext;
        private SQLiteDatabase mDatabase;
        private boolean mIndexCreated = false;
        private long mLastDocID=0;

        private long mIndexedSize;
        private long mTotalSize;
        private float mProgress;

        public DatabaseOpenHelper (String dir, String name,
                                   SQLiteDatabase.CursorFactory factory, int version) {
            super(dir, name, factory, version);
        }


        public boolean isIndexCreated () {return mIndexCreated;}



       /* DatabaseOpenHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
            //mHelperContext = context;
        }*/

        @Override
        public void onCreate(SQLiteDatabase db) {
            mDatabase = db;
            mDatabase.execSQL (FTS_CONTENT_TABLE_CREATE);
            mDatabase.execSQL (FTS_TABLE_CREATE);
            mDatabase.execSQL (FILE_STATUS_TABLE_CREATE);

            //loadAndCreateIndex();
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
            mDatabase = db;

            //loadAndCreateIndex();

        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.w(TAG, "Upgrading database from version " + oldVersion + " to "
                    + newVersion + ", which will destroy all old data");
            db.execSQL("DROP TABLE IF EXISTS " + FTS_VIRTUAL_TABLE);
            onCreate(db);
        }



        public boolean insertLineIndex (long fid, String fpath, long loc, String line) {

            synchronized (this) {
                if (!mDatabase.isOpen())
                    return false;

                ContentValues initialValues = new ContentValues();

                // initialValues.put(COL_DOCID, r1);//mLastDocID);
                initialValues.put(COL_FILEID, fid);
                initialValues.put(COL_FILEPATH, fpath);
                //initialValues.put(COL_FILEPATH, (String)null);
                initialValues.put(COL_LOCATION, loc);
                initialValues.put(COL_TEXT, (String)null);

                long r1 = mDatabase.insert(FTS_CONTENT_TABLE, null, initialValues);

                //initialValues.put(COL_DOCID, mLastDocID);
                //initialValues.put(COL_DOCID, "adfb/dftet.txt-" + String.valueOf(mLastDocID));
                initialValues.clear();

                initialValues.put(COL_DOCID, r1);//mLastDocID);
                initialValues.put(COL_FILEID, 0);
                initialValues.put(COL_FILEPATH, (String)null);
                initialValues.put(COL_LOCATION, 0);
                initialValues.put(COL_TEXT, getTokenizedText(line, null));

                long r2 = 0;

                mDatabase.insert(FTS_VIRTUAL_TABLE, null, initialValues);

                mLastDocID++;

                return (r1 >= 0 && r2 >= 0);
            }
        }


        private class TextFile {
            long id;
            String path;   // relative path
            long   modified;   // modified date
            long size;     // size
            long indexpos;

            public ContentValues toValues () {
                ContentValues values = new ContentValues();
                //values.put(COL_FILEID, id);
                values.put(COL_FILEPATH, path);
                values.put(COL_FILESIZE, size);
                values.put(COL_MODIFIED_TIME, modified);
                values.put(COL_INDEXPOS, indexpos);

                return values;
            }

            public TextFile (long id, String path, long size, long modified,long indexpos) {
                this.id = id;
                this.path = path;
                this.modified = modified;
                this.size = size;
                this.indexpos = indexpos;
            }

            public TextFile (ContentValues values) {
                this (values.getAsLong(COL_FILEID),
                        values.getAsString (COL_FILEPATH),
                        values.getAsLong(COL_FILESIZE),
                        values.getAsLong (COL_MODIFIED_TIME),
                        values.getAsLong(COL_INDEXPOS));
            }

            public String toString () {
                return "ID-" + id +
                        "PATH-" + path +
                        "SIZE-" + size +
                        "TIME-" + modified +
                        "IDXPOS-" + indexpos;
            }
        }

        private List <TextFile> getFileList(String path) {

            List <TextFile> files = new ArrayList<TextFile>();

            File parentDir = new File (path);
            String[] fileNames = parentDir.list();

            for (String fileName : fileNames) {
                if (fileName.toLowerCase().endsWith(".txt")) {

                    String filepath = path + "/" + fileName;
                    File f = new File(filepath);

                    try {
                        files.add( new TextFile (-1,
                                filepath,
                                f.length(),
                                f.lastModified(),
                                0));

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    File file = new File(parentDir.getPath() + "/" + fileName);
                    if (file.isDirectory()) {
                        files.addAll(getFileList(file.getAbsolutePath().toString()));
                    }
                }
            }

            return files;
        }

        // start or resume the index creation
        private void loadAndCreateIndex() {

            // get all text files in the dir
            List <TextFile> files = getFileList (mDir);

            // get total size
            mTotalSize = 0;
            for (TextFile file: files) mTotalSize += file.size;

            // load the file information database
            String [] columns = {
                    COL_ROWID, COL_FILEPATH, COL_FILESIZE, COL_MODIFIED_TIME, COL_INDEXPOS
            };

            Cursor c = null;
            synchronized (this) {
                 c = mDatabase.query(FILE_STATUS_TABLE, columns,
                        null, null, null, null, null);
            }

            Map <String, TextFile> fileMap = new TreeMap <String, TextFile> ();

            c.moveToFirst();
            while (!c.isAfterLast()) {

                TextFile file_db = new TextFile(
                        c.getLong(0), // file row id
                        c.getString(1), // file path
                        c.getLong(2),   // file size
                        c.getLong(3),  // modified time
                        c.getLong(4));

                fileMap.put(file_db.path, file_db);

                System.out.println ("File in Database: " + file_db);

                c.moveToNext();
            }

            // compare each file in the folder, find out new or modified one
            List <TextFile> filesToIndex = new ArrayList<TextFile> ();

            mIndexedSize = 0;

            for (TextFile file : files) {

                if (file==null) continue;

                TextFile fileDB = fileMap.get(file.path);

                if (fileDB == null ||
                    file.modified != fileDB.modified ||
                    file.size != fileDB.size) {
                    // a new file, or updated file

                    System.out.println ("File in DB: " + fileDB);
                    System.out.println ("File in folder: " + file);

                    // insert the file information into the table
                    ContentValues values = file.toValues();
                    long id = 0;

                    synchronized (this) {
                        id = mDatabase.insert(FILE_STATUS_TABLE, null, values);
                    }

                    if (id>=0) {
                        file.id = id;
                        fileMap.put (file.path, file);
                        fileDB = file;
                    }
                }

                // now fileDB is the status of the file

                // calculate total of indexed
                mIndexedSize += fileDB.indexpos;

                // check if index creation completed

                if (fileDB.indexpos < fileDB.size) {
                    // not completed, add to work queue
                    filesToIndex.add(fileDB);
                }

            }

            // update current progress
            mProgress = (float)mIndexedSize / mTotalSize;

            System.out.println ("Now loading index for " + filesToIndex.size() + " files.");

            // for each new or updated file, build the index
            for (TextFile file : filesToIndex) {
                if (mAsyncTask.isCancelled()) break;

                loadAndCreateIndex(file.id, file.path, file.indexpos);
            }

            // load and create index for each new or update file

        }

        // load and create index in file path, starting from location pos
        private void loadAndCreateIndex(long rowID, String path, long pos) {

            //mLastDocID = 0;


            RandomAccessTextFile file = null;

            try {

                file = new RandomAccessTextFile(path);
                file.setPosition(pos);

                long size = file.getSize();

                ContentValues newPosValues = new ContentValues();

                String line;
                mIndexCreated = true;
                float lastProgress = mProgress;

                System.out.println ("indexed:" + mIndexedSize + "; total:" + mTotalSize +"; progress:" + mProgress);
                mAsyncTask.updateProgress(new Float(mProgress));


                while ( !file.isEnd() ) {

                    if (mAsyncTask.isCancelled())
                        break;

                    long loc = file.getPosition();

                    line = file.readLine();

                    long newLoc = file.getPosition();

                    mIndexedSize += newLoc - loc;

                    mProgress = (float) mIndexedSize / mTotalSize;

                    if (mProgress - lastProgress > 0.05) {
                        System.out.println ("indexed:" + mIndexedSize + "; total:" + mTotalSize +"; progress:" + mProgress);
                        mAsyncTask.updateProgress(new Float(mProgress));
                        lastProgress = mProgress;
                    }


                    if (line.trim().isEmpty()) continue;

                    if (!insertLineIndex(rowID, path, loc, line)) {
                        mIndexCreated = false;
                        break;
                    }

                    newPosValues.clear();
                    newPosValues.put (COL_INDEXPOS, newLoc);
                    synchronized ( this) {
                        mDatabase.update(FILE_STATUS_TABLE,
                                newPosValues,
                                "rowid = ?",
                                new String[]{String.valueOf(rowID)}
                        );
                    }

                    //float new_progress = ((float)loc) / size;

                    if (mAsyncTask.isCancelled())
                        break;

                }

                if (file.isEnd()) {
                    newPosValues.clear();
                    newPosValues.put(COL_INDEXPOS, size);

                    synchronized (this) {
                        mDatabase.update(FILE_STATUS_TABLE,
                                newPosValues,
                                "rowid = ?",
                                new String[]{String.valueOf(rowID)}
                        );
                    }
                }

                System.out.println ("indexed:" + mIndexedSize + "; total:" + mTotalSize +"; progress:" + mProgress);
                mAsyncTask.updateProgress(new Float(mProgress));

            } catch (IOException e) {
                e.printStackTrace();
                mIndexCreated = false;
            } finally {
                if (file!=null)
                    file.close();
            }

        }
    }


    private boolean loadDatabase(BackgroundCreateIndexTask asyncTask) {

        mAsyncTask = asyncTask;

        SQLiteDatabase database = mDatabaseOpenHelper.getWritableDatabase();

        if (database!=null) {
            mDatabaseOpenHelper.loadAndCreateIndex();

            return mDatabaseOpenHelper.isIndexCreated();
        }
        else
            return false;

    }

    private static final String SELECTION = COL_TEXT + " MATCH ?";

    private static final String[] PROJECTION = new String[] {
            COL_DOCID + " AS " + BaseColumns._ID ,
            COL_FILEID,
            COL_FILEPATH,
            COL_LOCATION
           // COL_DOCID + " AS " + COL_LOCATION
    };

    private static final String[] COLUMNS =
            new String [] {
                    BaseColumns._ID,
                    COL_BOOKTITLE,
                    COL_TEXT};

    private String getTextFromFile (String path, long pos) {
        RandomAccessTextFile file = null;

        try {

            file = new RandomAccessTextFile(path);
            file.setPosition(pos);
            return file.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (file!=null) file.close();
        }
    }

    public Cursor searchText (String query) {

        if (query == null) return null;

        ArrayList<String> words = new ArrayList<String> ();

        String[] selectionArgs = new String[]{// query };
                getTokenizedText(query,words)}; //todo deal with "", AND, OR etc

        SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
        builder.setTables(FTS_VIRTUAL_TABLE);

        Cursor cursor = null;

        synchronized (this) {
            if (mDatabaseOpenHelper.mDatabase.isOpen())
                cursor = builder.query(mDatabaseOpenHelper.getReadableDatabase(),
                      PROJECTION, SELECTION, selectionArgs, null, null, null);
        }

        if (cursor == null) {
            return null;
        }  else {
            // Build a cursor, adding the real Text
            MatrixCursor cursor_new = new MatrixCursor(COLUMNS);

            cursor.moveToFirst();

            Object[] row = new Object[3];

            while (!cursor.isAfterLast()) {
                row[0] = cursor.getLong(0);  // ID
                String path = cursor.getString(2);
                long pos = cursor.getLong(3);  // Location

                int i = path.lastIndexOf('/');
                int j = path.lastIndexOf('.');

                if (i>0 & j>i)
                    row[1] = path.substring(i+1,j);
                else if (i>0 && j<0)
                    row[1] = path.substring(i+1);
                else
                    row[1] = path;

                String para = getTextFromFile(path, pos);

                if (para!=null) {
                    String formatedTxt = getHighlightedText(para, words);

                    row[2] = formatedTxt;

                    cursor_new.addRow(row);
                }

                cursor.moveToNext();
            }

            return cursor_new;
        }
    }

 /*   public Cursor getWordMatches(String query, String[] columns) {
        if (query!=null) {
            String selection = COL_WORD + " MATCH ?";
            String[] selectionArgs = new String[]{query};

            return query(selection, selectionArgs, columns);
        }
            return query (null, null, columns);
    }

    private Cursor query(String selection, String[] selectionArgs, String[] columns) {
        SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
        builder.setTables(FTS_VIRTUAL_TABLE);

        Cursor cursor = builder.query(mDatabaseOpenHelper.getReadableDatabase(),
                columns, selection, selectionArgs, null, null, null);

        if (cursor == null) {
            return null;
        } else if (!cursor.moveToFirst()) {
            cursor.close();
            return null;
        }
        return cursor;
    }*/

    public static class HTMLTextCursorAdapter extends SimpleCursorAdapter {
        HTMLTextCursorAdapter(Context context, int layout, Cursor c,
                              String[] from, int[] to, int flags) {
            super (context, layout, c, from, to, flags);
        }

        @Override
        public void setViewText (TextView v, String text) {
            v.setText(Html.fromHtml(text));
        }
    }

    public static class TextQueryLoader extends CursorLoader {
        private String mTxt;
        DatabaseTable mTable;

        TextQueryLoader (Context context, DatabaseTable table, String txt) {
            super (context);
            mTable = table;
            mTxt = txt;
        }

        @Override
        public Cursor loadInBackground () {
            return mTable.searchText(mTxt);
        }
    }

    public void deleteDatabase() {
        // TODO
        synchronized (this) {
            mDatabaseOpenHelper.close();
        }

        File f = new File (mDir + DATABASE_FILE);

        f.delete();
    }

    public void closeDatabase() {

        if (mAsyncTask!=null) mAsyncTask.cancel(true);

        synchronized (this) {
            mDatabaseOpenHelper.close();
        }
    }

}
