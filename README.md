# db


DBヘッダ
    先頭??byte  ファイルシグネチャ(?)
    続く2byte   DBヘッダサイズ(シグネチャ含むDBヘッダ全体)
    続く2byte   ファイルフォーマットバージョン番号
    続く4byte   テーブルIDテーブルの先頭エントリーのアドレス(エントリー部内相対アドレス)
    続く4byte   再利用可能(サイズ20byte以上の)エントリー(Tree型で管理)のルートノードのアドレス(エントリー部内相対アドレス)
    続く4byte   再利用可能(サイズ16byteの)エントリー(List型で管理)の先頭ノードのアドレス(エントリー部内相対アドレス)
    続く4byte   エントリー部全体のサイズ
エントリー部
    エントリーが並ぶ(List型やTree型のどちらも混ざって並ぶ、アドレス保持により連結するため混ざって配置されても無問題)
    

エントリーヘッダ
    先頭2byte    エントリーサイズ(ヘッダ含む)(4の倍数のバイト数になるようにする)
    続く2byte    テーブルID

List型エントリー
    エントリーヘッダ
    続く4byte    前のエントリーのアドレス(エントリー部内相対アドレス)
    続く4byte  　次のエントリーのアドレス(エントリー部内相対アドレス)
    以降、テーブルによって定められたカラムデータが並ぶ

Tree型エントリー
    エントリーヘッダ
    続く4byte   親のエントリーのアドレス(エントリー部内相対アドレス)
    続く4byte   左の子のエントリーのアドレス(エントリー部内相対アドレス)
    続く4byte   右の子のエントリーのアドレス(エントリー部内相対アドレス)
    続く1byte   高さ
    続く3byte   サブツリーノード総数
    以降、テーブルによって定められたカラムデータが並ぶ



変更ごとに毎度細かいファイル編集が良いかどうかわからん
たとえば適当なチャンク(10KBとか？)に区切って読み書きするのは？
全データをメモリにロードするのは？重すぎる？
全データをメモリにロードするなら普通のデータ構造でもいい気がする

適切なチャンクサイズの設定は難易度高そう
HDDアクセスやOSの読み書き処理の仕組みを知る必要があり
正直、それ調べるの面倒
軽くググったがわからん
雰囲気的にあんま気にしなくてよさそう？
仮にチャンク作るとしたら4KB,8KB,16KBのいずれかがよさそうな雰囲気・・

Goのメモリバッファ(bytes.Buffer)にio.Seekerが実装されてないため
チャンクに対する操作は厳しいかもしれない

Goでプリミティブをbyte列への変換は
encoding/binaryパッケージを使う
(インターフェース経由、なんだよね・・・)

テーブルIDテーブル
および
テーブルIDテーブルのカラム
および
カラムの定義方法
を
決めないと・・・

