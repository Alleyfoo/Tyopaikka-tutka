from apprscan.cli import main


def test_cli_help(capsys):
    code = main([])
    captured = capsys.readouterr()
    assert code == 0
    assert "apprscan" in captured.out
    assert "run" in captured.out
