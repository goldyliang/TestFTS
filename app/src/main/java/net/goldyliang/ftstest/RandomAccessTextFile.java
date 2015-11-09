package net.goldyliang.ftstest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 * Created by gordon on 10/25/15.
 */
public class RandomAccessTextFile {

    private RandomAccessFile mFile;
    private FileChannel mChannel;
    private ByteBuffer mBuffer;
    private long mBufFilePos; // position in file at the beginning of the buffer
    private int mBufLastPos;  // position in the buffer after last output
    private StringBuffer mStringBuf;
    private byte[] mBufArray;
    private boolean mEnd = false;

    private static int BUFFER_SIZE = 1024;

    RandomAccessTextFile (String path) throws FileNotFoundException, IOException {
        mFile = new RandomAccessFile (path, "r");
        mChannel = mFile.getChannel();

        mBufArray = new byte[BUFFER_SIZE];

        mBuffer = ByteBuffer.wrap(mBufArray);

        mStringBuf = new StringBuffer();
        mBufLastPos = 0;
        mBufFilePos = mChannel.position();
        mEnd = false;
    }

    boolean isEnd() {
        return mEnd;
    }

    long getSize() throws IOException {
        return mFile.length();
    }

    String readLine() throws IOException {
        //mBuffer.clear();
        //  StringBuffer buf = new StringBuffer();

        //  buf.append(mBuffer.asCharBuffer().array(), mPosBuf, mBuffer.position() - mPosBuf);

        if (mEnd) return null;

        do {
            // current buffer position
            int bufPos = mBuffer.position();

            // if nothing in the buffer, loop
            if (bufPos ==0) continue;

            // scan last output position to the end of buffer
            for (int i=mBufLastPos; i<bufPos; i++) {
                char c = (char)mBuffer.get(i);
                if ( c == '\n' || c == '\r' ) {

                    // get a new line
                    //byte [] arr = new byte[BUFFER_SIZE];

                    //mBuffer.get ( arr, mBufLastPos, i - mBufLastPos);

                    String s = new String (mBufArray, mBufLastPos, i - mBufLastPos, Charset.forName("UTF-8"));
                    mStringBuf.append(s);
                	
                  /*  mStringBuf.append ( mBuffer.array(),
                            mBufLastPos,
                            i - mBufLastPos); */

                    // check if we have another upcoming \n or \r
                    if (i+1 < bufPos) {
                        char c1 = (char) mBuffer.get(i + 1);
                        if (c1 == '\n' || c1 == '\r')
                            i++; //skip this char if yea
                    }

                    mBufLastPos = i + 1; // next invoke scan from here

                    String line = mStringBuf.toString();
                    mStringBuf = new StringBuffer();
                    return line;
                }
            }

            // we have not get a new line up here

            // append the buffer from mBuffer to the end, to string buffer
            String s = new String (mBufArray, mBufLastPos, bufPos - mBufLastPos, Charset.forName("UTF-8"));
            mStringBuf.append(s);


            // scan from zero next loop
            mBufLastPos = 0;

            // mark current file position
            mBufFilePos = mChannel.position();

            // clear the buffer
            mBuffer.clear();

        }  while (mChannel.read(mBuffer) > 0); // read from file

        String line = mStringBuf.toString();
        mEnd = true; // indicating end of file

        return line;
    }

    long getPosition() {
        return mBufFilePos + mBufLastPos;
    }

    void setPosition(long pos) throws IOException {
        mChannel.position(pos);
        mBufFilePos = pos;
        mBufLastPos = 0;

        mStringBuf = new StringBuffer();
    }

    void close() {
        try {
            if (mChannel != null) {
                mChannel.close();
                mChannel = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (mFile !=null) {
                mFile.close();
                mFile = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        mEnd = true;
    }

    public static void main(String[] args) {

        RandomAccessTextFile file = null;

        try {
            file = new RandomAccessTextFile ("/home/gordon/egw_books_txt/bb/121.txt");

            while (!file.isEnd()) {
                long pos = file.getPosition();
                String line = file.readLine();

                System.out.println ("Pos:" + pos);
                System.out.println (line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (file!=null) file.clone();} catch (Exception e) {}
        }
    }

}
