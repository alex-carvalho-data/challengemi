
import statistics

def compute_insights(hearing_test_result):
    """
    Computes the insights for a hearing test result. 
    In the future we may want to modify this function and add new insights

    Parameters
    ----------
    hearing_test_result : dict
        One of the entries from the 'results' array in a hearing test record.

    Returns
    -------
    insights : dict
        An object containing computed insights, for now we are only computing the `PTA4` insight: 
        see: https://www.nidcd.nih.gov/health/statistics/what-numbers-mean-epidemiological-perspective-hearing#tests
    """
    pta4_frequencies = [500, 1000, 2000, 4000]
    return {
        'PTA4': statistics.mean([
            audiogram['threshold_db'] for audiogram in hearing_test_result['audiograms']
            if audiogram['freq_hz'] in pta4_frequencies
        ])
    }



def clean_hearing_test(hearing_test):
    """
    Cleans a hearing test. 
    
    Parameters
    ----------
    a hearing_test : dict
        A hearing test record
    """
    # Clean user's year_of_birth, it strips surrounding "optional()" string and sets an integer
    yob_string = hearing_test['user']['year_of_birth']
    if yob_string.lower().startswith('optional('):
        hearing_test['user']['year_of_birth'] = int(yob_string.split("(")[1][:-1])
    else:
        hearing_test['user']['year_of_birth'] = int(yob_string)



