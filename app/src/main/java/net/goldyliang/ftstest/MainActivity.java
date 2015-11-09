package net.goldyliang.ftstest;

import android.app.ListActivity;
import android.app.LoaderManager;
import android.content.Loader;
import android.database.Cursor;
import android.os.Environment;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.ProgressBar;
import android.widget.TextView;

import net.digielec.reader.testfts.R;

import java.io.File;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
//import net.digielec.reader.testsqlite.DatabaseTable;


public class MainActivity extends ListActivity implements LoaderManager.LoaderCallbacks<Cursor> {

    DatabaseTable db;



//    Button mSearchButton;
    EditText mSearchText;
    ListView mSearchResults;
    ProgressBar mProgressBar;

    DatabaseTable.HTMLTextCursorAdapter mAdapter;


    class MyLoaderClass extends DatabaseTable.BackgroundCreateIndexTask {
        @Override
        protected void onProgressUpdate (Float... values) {
            mProgressBar.setProgress( (int) (values[0] * 100) );
            System.out.println ("Index completed " + values[0]*100 + "%");
        }

        @Override
        protected void onPostExecute(Boolean result) {
            System.out.println ("Index loader returned: " + result);
        }

        @Override
        protected void onCancelled(Boolean result) {
            System.out.println ("Index loader canceled, returned: " + result);
        }
    }

    MyLoaderClass mLoader;

    public MainActivity() {
        super();

        File sdcard = Environment.getExternalStorageDirectory();

        String path = sdcard.toString() + "/books/";

        File folder = new File (path);

        if (folder.exists())
            db = new DatabaseTable(path,
                    BreakIterator.getCharacterInstance());
                    //BreakIterator.getWordInstance());

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        mSearchText = (EditText)findViewById(R.id.searchText);
        mProgressBar = (ProgressBar) findViewById (R.id.progressBar);
        mProgressBar.setMax(100);

        mSearchResults = this.getListView();

        String[] fromColumns = {DatabaseTable.COL_BOOKTITLE, DatabaseTable.COL_TEXT};// {BaseColumns._ID};
        int[] toViews = {R.id.searchedTextLocation, R.id.searchedTextParagraph};

        mAdapter = new DatabaseTable.HTMLTextCursorAdapter(this,
                R.layout.search_result, null, fromColumns, toViews, 0);
        this.setListAdapter(mAdapter);

        // TODO: remove
        //db.deleteDatabase();

        if (db!=null) {
            mLoader = new MyLoaderClass();

            mLoader.execute(db);
        }

        TextView vw = (TextView) findViewById(R.id.textView);
        List <String> list = new ArrayList<String> ();
        db.getTokenizedText("乌鸦", list);

        StringBuffer buf = new StringBuffer();
        for (String word:list)
            buf.append(word + " ");

        vw.setText(buf);


        /*BreakIterator it = BreakIterator.getCharacterInstance(); //.getWordInstance(Locale.SIMPLIFIED_CHINESE);
        String txt = "我们一起";
        it.setText(txt);
        int start = it.first();
        int end = it.next();

        buf = new StringBuffer();

        while (end != BreakIterator.DONE) {
            String word = txt.substring(start,end).trim();
            if (!word.isEmpty()) {
                buf.append(word);
                buf.append("+");
            }
            start = end;
            end = it.next();
        }

        vw.setText(buf);*/

    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        } else if (id == R.id.action_clean) {

        }

        return super.onOptionsItemSelected(item);
    }

    @Override
    protected void onDestroy() {

        //mLoader.cancel(true);

        //System.err.println("Delete database");

        //db.deleteDatabase();
        //this.deleteDatabase(DatabaseTable.DATABASE_NAME);
        if (db!=null)
            db.closeDatabase();

        super.onDestroy();
    }

    public void doClear(View view) {

        if (db==null) return;

        db.deleteDatabase();

        mLoader = new MyLoaderClass();

        mLoader.execute(db);
    }

    public void doSearch(View view) {

       // String text = mSearchText.getText().toString();
        //String text = "\"教会\" \"邀请\"";
        String text = "\"发怨言\" \"旷野\"";
        //String text = "乌鸦";

        System.out.println (mSearchText.getText());

//        Cursor c = db.searchText( text );

        Bundle args = new Bundle();
        args.putString("query", text);
        getLoaderManager().initLoader(0, args, this);

        /*
        int cnt=0;

        while (!c.isAfterLast() && cnt<=10) {
            long loc = c.getLong(0);
            String txt = c.getString(1);

            System.err.println("Loc :" + loc);
            System.err.println("Text :" + txt);

            TextView v = new TextView (this);
            v.setText(txt);

            cnt ++;

            mSearchResults.addFooterView(v);

            c.moveToNext();
        } */

    }

    // Called when a new Loader needs to be created    @Override
    @Override
    public Loader<Cursor> onCreateLoader(int id, Bundle args) {
        // Now create and return a CursorLoader that will take care of
        // creating a Cursor for the data being displayed.
        return new DatabaseTable.TextQueryLoader(this, db, args.getString("query"));
    }

    // Called when a previously created loader has finished loading
    @Override
    public void onLoadFinished(Loader<Cursor> loader, Cursor data) {
        // Swap the new cursor in.  (The framework will take care of closing the
        // old cursor once we return.)
        mAdapter.swapCursor(data);
    }

    // Called when a previously created loader is reset, making the data unavailable
    //public void onLoaderReset (Loader<D> loader)

    public void onLoaderReset(Loader<Cursor> loader) {
        // This is called when the last Cursor provided to onLoadFinished()
        // above is about to be closed.  We need to make sure we are no
        // longer using it.
        mAdapter.swapCursor(null);
    }

}
