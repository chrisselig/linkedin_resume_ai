"""Education panel UI module for the LinkedIn resume Shiny application.

Provides Shiny module functions to display education entries.
"""

import pandas as pd
from shiny import module, ui, render, reactive


def _build_education_card(row: pd.Series) -> ui.Tag:
    """Build a single education card HTML element from a DataFrame row.

    Args:
        row: A pandas Series representing one education record.
             Expected keys: school, degree, field_of_study, start_year,
             end_year, description.

    Returns:
        An htmltools Tag representing the education card.
    """
    start_year = str(row.get("start_year", "")).strip()
    end_year = str(row.get("end_year", "")).strip()

    if start_year and end_year:
        year_range = f"{start_year} – {end_year}"
    elif start_year:
        year_range = f"{start_year} – Present"
    else:
        year_range = end_year

    degree = str(row.get("degree", "")).strip()
    field = str(row.get("field_of_study", "")).strip()
    degree_line = f"{degree}, {field}" if degree and field else degree or field

    description = str(row.get("description", "")).strip()

    return ui.div(
        ui.h3(str(row.get("school", "")), class_="edu-school"),
        ui.p(degree_line, class_="edu-degree") if degree_line else ui.span(""),
        ui.p(year_range, class_="edu-years") if year_range else ui.span(""),
        ui.p(description, class_="edu-description") if description else ui.span(""),
        class_="edu-card",
    )


@module.ui
def education_ui() -> ui.Tag:
    """Shiny module UI for the education section.

    Returns:
        A Shiny Tag containing the education section placeholder.
    """
    return ui.div(
        ui.h2("Education", class_="section-heading"),
        ui.output_ui("education_cards"),
        class_="section education-section",
    )


@module.server
def education_server(input, output, session, data: reactive.Value) -> None:
    """Shiny module server logic for the education section.

    Args:
        input: Shiny input object (unused but required by module contract).
        output: Shiny output object.
        session: Shiny session object (unused but required by module contract).
        data: A reactive.Value containing a pandas DataFrame with education
              records. Expected columns: school, degree, field_of_study,
              start_year, end_year, description.
    """

    @output
    @render.ui
    def education_cards() -> ui.Tag:
        """Render education cards from the provided data reactive."""
        df = data()
        if df is None or df.empty:
            return ui.p("No education data available.", class_="no-data")
        cards = [_build_education_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="edu-list")
