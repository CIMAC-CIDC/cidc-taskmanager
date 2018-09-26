#!/usr/bin/env python3
"""
Module for tasks that do post-run processing of output files.
"""
import logging
import subprocess
from typing import List, Generator, NamedTuple
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

OLINK_FIRST_COLUMN = [None, "NPX data", "Panel", "Assay", "Uniprot ID", "OlinkID"]


class SampleInfo(NamedTuple):
    """
    Basic store for sample related info.

    Attributes:
        plate_gen {Generator} -- plate info
        qc_gen {Generator} -- qc info
        sample_ids {List[str]} -- id list

    Arguments:
        NamedTuple {[type]} -- [description]
    """

    plate_gen: Generator
    qc_gen: Generator
    sample_ids: List[str]


class AssayColumn(NamedTuple):
    """
    Structure for holding all the generators needed to get assay information
    from olink files.
    """

    assay_info_gen: Generator
    assay_metrics_gen: Generator
    data_col_gen: Generator


def validate_row_headers(generator: Generator, error_list: List[dict]) -> int:
    """
    Validates the first row of an olink file. Also returns the row index of the olinkID field.

    Arguments:
        generator {generator} -- Iterable generator object representing a column of cells.
        error_list {List[dict]} -- List of errors for the document

    Raises:
        InvalidFileException -- [description]

    Returns:
        int -- row index of the olinkID field.
    """
    field_labels = []
    olink_row = None

    while not olink_row:
        cell = next(generator)[0]
        field_labels.append(cell.value)
        if cell.value == "OlinkID":
            olink_row = cell.row

    if field_labels != OLINK_FIRST_COLUMN or not olink_row:
        error_list.append(
            {
                "explanation": "The field names in the first column do not match expected names",
                "affected_paths": [],
                "raw_or_parse": "PARSE",
                "severity": "CRITICAL",
            }
        )
        raise InvalidFileException(error_list[-1]["explanation"])

    return olink_row


def validate_qc_info(generators: SampleInfo, errors: List[dict]) -> List[dict]:
    """
    Loops over the qc/plateid info and returns it in a formatted list.

    Arguments:
        generators {SampleInfo} -- Simple namedtuple.
        errors {List[dict]} -- List of errors.

    Returns:
        List[dict] -- List of SAMPLE_SCHEMA structured dicts.
    """
    sample_list = []
    try:
        # Generator should have a single column.
        plate_col = next(generators.plate_gen)
        qc_col = next(generators.qc_gen)
        for plate_cell, qc_cell, sample_id in zip(
            plate_col, qc_col, generators.sample_ids
        ):
            sample_list.append(
                {
                    "sample_id": sample_id,
                    "qc_status": qc_cell.value,
                    "plate_id": plate_cell.value,
                }
            )
    except StopIteration:
        errors.append(
            {
                "explanation": "Unable to find the plate or qc column",
                "affected_paths": ["samples.qc_status", "samples.plate_id"],
                "raw_or_parse": "PARSE",
                "severity": "WARNING",
            }
        )
    return sample_list


def extract_assay_data(
    assay_obj: AssayColumn, sample_ids: List[str], errors: List[dict]
) -> List[dict]:
    """
    Takes a generator of assay info cells and data containing cells, and returns the
    data in a formatted manner.

    Arguments:
        assay_obj {AssayColumn} -- namedtuple that holds all the generators for data processing.
        sample_ids {List[str]} -- List of sample_ids.
        errors {List[dict]} -- A list of parsing errors that have been found.

    Raises:
        InvalidFileException -- Indicates to the calling function that this file is invalid.

    Returns:
        List[dict] -- List of olink assay structured dicts.
    """
    assay_list = []
    for assay_def, data, metrics in zip(
        assay_obj.assay_info_gen, assay_obj.data_col_gen, assay_obj.assay_metrics_gen
    ):
        # Create the assay definition.
        olink_assay = {
            "panel": assay_def[0].value,
            "assay": assay_def[1].value,
            "uniprot_id": assay_def[2].value,
            "olink_id": assay_def[3].value,
            "lod": metrics[0].value,
            "missing_data_freq": metrics[1].value,
            "results": [],
        }

        # Sanity check sample number.
        if len(data) != len(sample_ids):
            errors.append(
                {
                    "explanation": (
                        "There is a mismatch between the number of samples and the"
                        "number of data points for assay %s" % olink_assay["assay"]
                    ),
                    "affected_paths": [],
                    "raw_or_parse": "PARSE",
                    "severity": "CRITICAL",
                }
            )
            raise InvalidFileException(errors[-1]["explanation"])

        # Create a new entry in the assay list.
        # Note that the check for qc/lod checks for any variation from a black-text no-fill
        # cell. If other colors are used to indicate other things, this implementation will
        # have to be changed.
        for i, data_cell in enumerate(data):
            olink_assay["results"].append(
                {
                    "sample_id": sample_ids[i],
                    "value": data_cell.value,
                    "qc_fail": data_cell.font.color is not None,
                    "below_lod": data_cell.fill.bgColor.rgb != "00000000",
                }
            )
        assay_list.append(olink_assay)
    return assay_list


def add_file_extension(path: str, extension: str) -> str:
    """[summary]
    
    Arguments:
        path {str} -- [description]
        extension {str} -- [description]
    
    Returns:
        str -- [description]
    """
    newpath = '%s.%s' % (path, extension)
    args = ['mv', path, newpath]
    subprocess.run(args)
    return newpath


def process_olink_npx(path: str, trial_id: str, assay_id: str, record_id: str) -> dict:
    """
    Processes an olink npx file and creates a record.

    Arguments:
        path {str} -- Location of the file.
        trial_id {str} -- Trial id.
        assay_id {str} -- Assay id.
        record_id {str} -- ID of the record in the data collection.

    Returns:
        dict -- olink data formatted for mongodb.
    """
    olink_record = {"trial": trial_id, "assay": assay_id, "record_id": record_id}
    errors = []
    sample_ids = []
    sample_cells = []
    samples_start_row = None
    samples_end_row = None
    olink_row = None
    mdf_row = None

    xlsx_path = add_file_extension(path, 'xlsx')

    try:
        # Try to load the file.
        workb = load_workbook(xlsx_path)
        # Load the sheet
        wks = workb.active
        # Grab cells from column A
        cell_gen = wks.iter_rows(max_col=1)
        olink_row = validate_row_headers(cell_gen, errors)

        for sam_cell in cell_gen:
            value = sam_cell[0].value
            if value == "Missing Data freq.":
                mdf_row = sam_cell[0].row
            # If a value is a sampleID, note it down.
            elif value == "Panel":
                # Quick way to get the panel name
                olink_record["ol_panel_type"] = wks[sam_cell[0].row][1].value
            elif value and value != "LOD":
                sample_cells.append(sam_cell[0])
                sample_ids.append(value)

        if not mdf_row:
            errors.append(
                {
                    "explanation": (
                        "The field names in the first column do not match expected"
                        "names"
                    ),
                    "affected_paths": [],
                    "raw_or_parse": "PARSE",
                    "severity": "CRITICAL",
                }
            )
            raise InvalidFileException("MDF row not found")

        # Store values to know what cell range the sample values are in.
        samples_start_row = sample_cells[0].row
        samples_end_row = sample_cells[-1].row

        # Get the assay info. Stop before hitting the plate and qc columns.
        assay_info_gen = wks.iter_cols(
            min_col=2,
            max_col=wks.max_column - 2,
            min_row=olink_row - 3,
            max_row=olink_row,
        )

        # Get assay metrics.
        assay_metrics_gen = wks.iter_cols(
            min_col=2, max_col=wks.max_column - 2, min_row=mdf_row - 1, max_row=mdf_row
        )

        # Get the data points. Stop before hitting the plate and qc columns.
        data_col_gen = wks.iter_cols(
            min_col=2,
            max_col=wks.max_column - 2,
            min_row=samples_start_row,
            max_row=samples_end_row,
        )

        # Get plate info.
        plate_gen = wks.iter_cols(
            min_col=wks.max_column - 1,
            max_col=wks.max_column,
            min_row=samples_start_row,
            max_row=samples_end_row,
        )

        qc_info = wks.iter_cols(
            min_col=wks.max_column,
            max_col=wks.max_column,
            min_row=samples_start_row,
            max_row=samples_end_row,
        )

        assay_info_obj = AssayColumn(assay_info_gen, assay_metrics_gen, data_col_gen)
        olink_record["ol_assay"] = extract_assay_data(
            assay_info_obj, sample_ids, errors
        )

        # Get the sample-specific information.
        info_obj = SampleInfo(plate_gen, qc_info, sample_ids)
        olink_record["samples"] = validate_qc_info(info_obj, errors)

        # Get NPX Manager Version
        olink_record["npx_m_ver"] = wks['B1'].value

    except InvalidFileException as err:
        log = (
            "Error loading file %s. This file is not a valid xlsx file. Error message: %s"
            % (path, str(err))
        )
        logging.warning({"message": log, "category": "WARNING-CELERY-FAIR-IMPORT"})
        errors.append(
            {
                "explanation": (
                    "The file was not recognized as a valid xlsx sheet and could not be"
                    "loaded."
                ),
                "affected_paths": [],
                "raw_or_parse": "RAW",
                "severity": "CRITICAL",
            }
        )

    olink_record["validation_errors"] = errors
    return olink_record
