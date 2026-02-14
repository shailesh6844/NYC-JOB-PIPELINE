import matplotlib.pyplot as plt
import plotly.express as px
import os


# ---------------------------------------------------------
# Matplotlib Horizontal Bar Chart Generator
# ---------------------------------------------------------
def matplotlib_bar(df, x, y, title, output_path=None, filename=None):
    """
    Generates a horizontal bar chart using Matplotlib from a Spark DataFrame.

    Steps:
    1. Converts Spark DataFrame to Pandas DataFrame.
    2. Limits visualization to top 20 records if dataset is large.
    3. Creates a horizontal bar chart.
    4. Saves the chart to disk (if path and filename are provided)
       or displays it interactively.

    Parameters:
        df (DataFrame): Spark DataFrame to visualize.
        x (str): Column name for y-axis categories.
        y (str): Column name for x-axis values.
        title (str): Title of the chart.
        output_path (str, optional): Directory to save the chart.
        filename (str, optional): File name for the saved chart.

    Behavior:
        - Uses horizontal bar chart (barh).
        - Automatically creates output directory if it does not exist.
        - Closes figure after saving to free memory.
        - Displays chart if no save location is provided.

    Returns:
        None
    """

    # Convert Spark DataFrame to Pandas for plotting
    pdf = df.toPandas()

    # Limit to top 20 records for readability
    if len(pdf) > 20:
        pdf = pdf.head(20)

    plt.figure(figsize=(12, 8))
    plt.barh(pdf[x], pdf[y])
    plt.title(title)
    plt.xlabel(y)
    plt.ylabel(x)
    plt.tight_layout()

    # Save plot if output path and filename are provided
    if output_path and filename:
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, filename)
        plt.savefig(file_path)
        print(f"Saved visualization to {file_path}")
        plt.close()  # Free memory after saving
    else:
        plt.show()


# ---------------------------------------------------------
# Plotly Interactive Horizontal Bar Chart Generator
# ---------------------------------------------------------
def plotly_bar(df, x, y, title, output_path=None, filename=None):
    """
    Generates an interactive horizontal bar chart using Plotly
    from a Spark DataFrame.

    Steps:
    1. Converts Spark DataFrame to Pandas DataFrame.
    2. Limits visualization to top 20 records if dataset is large.
    3. Creates an interactive horizontal bar chart.
    4. Saves the visualization as HTML or static image if specified.
       If static export fails, falls back to HTML.

    Parameters:
        df (DataFrame): Spark DataFrame to visualize.
        x (str): Column name for y-axis categories.
        y (str): Column name for x-axis values.
        title (str): Title of the chart.
        output_path (str, optional): Directory to save the chart.
        filename (str, optional): File name for the saved chart.

    Behavior:
        - Orientation set to horizontal ('h').
        - Saves as:
            • HTML file (if filename ends with .html)
            • Static image (requires Kaleido installed)
        - Automatically creates output directory.
        - Falls back to HTML export if static export fails.
        - Displays interactive chart if no save location is provided.

    Returns:
        None
    """

    # Convert Spark DataFrame to Pandas for plotting
    pdf = df.toPandas()

    # Limit to top 20 records for readability
    if len(pdf) > 20:
        pdf = pdf.head(20)

    # Create interactive horizontal bar chart
    fig = px.bar(
        pdf,
        x=y,
        y=x,
        orientation='h',
        title=title
    )

    # Save plot if output path and filename are provided
    if output_path and filename:
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, filename)

        # Save as HTML if specified
        if filename.endswith('.html'):
            fig.write_html(file_path)
        else:
            # Attempt static image export (requires Kaleido)
            try:
                fig.write_image(file_path)
            except Exception as e:
                print(f"Could not save static image for {filename}. Ensure kaleido is installed. Error: {e}")
                # Fallback to HTML format
                html_path = file_path.rsplit('.', 1)[0] + ".html"
                fig.write_html(html_path)
                print(f"Saved as HTML instead: {html_path}")
                return

        print(f"Saved visualization to {file_path}")
    else:
        fig.show()
