#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <ctime>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>

using namespace std;

int main(int argc, char *argv[])
{
    string ifname;
    string ofname;
    int numFiles = 0;
    int opt;
    while (-1 != (opt = getopt(argc, argv, "i:o:n:")))
    {
        switch (opt) {
            case 'i': // Name of the Tracefiles
                ifname = optarg;
                break;
            case 'o': // Name of the output file
                ofname = optarg;
                break;
            case 'n': // Number of Tracefiles
                numFiles = stoi(optarg);
                break;
            default:
                cerr << "Input Format Error!" << endl;
                exit(0);
        }
    }

    string baseName = ifname.substr(0, ifname.find_last_of("."));

    // Part to find out the Total amount of the Tracefiles
    // I added this part because I am sick of adding file sizes by my hand!!!
    long long totalSize = 0;
    for (int i = 1; i <= numFiles; i++) {
        string filePath = baseName + to_string(i) + ".txt";
        struct stat st;
        if (stat(filePath.c_str(), &st) == 0) {
            totalSize += st.st_size;
        } else {
            cerr << "Failed to get the size of " << filePath << endl;
        }
    }
    cout << "Total size of tracefiles: " << totalSize << " bytes" << endl;

    // Measuring time of the analysis phase srtar
    time_t start, finish;
    double duration;
    start = time(NULL);

    int totalCount = 0;
    unordered_map<string, int> umap;

    for (int i = 1; i <= numFiles; i++) {
        string filePath = baseName + to_string(i) + ".txt";
        cout << "Processing file: " << filePath << endl;
        ifstream ifile(filePath);
        cout << "[1] Opened the input file successfully" << endl;

        string line;
        while (getline(ifile, line))
        {
            char prefix[5] = "\0";
            strncpy(prefix, line.c_str(), 4);
            /* We do not need this part as PRISM does not print File names
            if(strcmp(prefix, "File") == 0) {
                continue;
            }*/
            umap[line]++;
            totalCount++;
        }

        ifile.close();
    }
    cout << " * number of fingerprints = " << totalCount << " * " << endl;
    cout << "[3] Calculating the duplicate-ratio . . ." << endl;

    int dedup_count = 0;
    for (auto x : umap)
    {
        if (x.second != 1) {
            dedup_count += (x.second - 1);
        }
    }
    double dedup_ratio = (dedup_count / (double) totalCount) * 100;

    cout << endl << "number of fingerprints:    " << totalCount << endl;
    cout << "number of duplicates:      " << dedup_count << endl;
    cout << "dedup-ratio:               " << dedup_ratio << " %" << endl;

    finish = time(NULL);
    duration = (double) (finish - start);

    ofstream ofile(ofname);
    if (ofile.is_open()) {
        ofile << "Base input file pattern:   " << baseName << endl << endl;
	cout << "Total size of tracefiles: " << totalSize << " bytes" << endl <<endl;
        ofile << "number of fingerprints:    " << totalCount << endl;
        ofile << "number of duplicates:      " << dedup_count << endl;
        ofile << "dedup-ratio:               " << dedup_ratio << " %" << endl << endl;
        ofile << "elapsed time:              " << duration << " seconds" << endl;
    }
    ofile.close();

    return 0;
}

