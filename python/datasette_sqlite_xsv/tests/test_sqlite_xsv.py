from datasette.app import Datasette
import pytest


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-sqlite-xsv" in installed_plugins

@pytest.mark.asyncio
async def test_sqlite_xsv_functions():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/_memory.json?sql=select+xsv_version(),xsv()")
    assert response.status_code == 200
    xsv_version, xsv = response.json()["rows"][0]
    assert xsv_version[0] == "v"
    assert len(xsv) == 26