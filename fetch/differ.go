package fetch

import "golang.org/x/xerrors"

type Differ struct {
	local  *MemoryChain
	remote *RemoteChain
}

func NewDiffer(local *MemoryChain, remote *RemoteChain) *Differ {
	return &Differ{
		local:  local,
		remote: remote,
	}
}

func (d *Differ) CalcDiffer() (uint64, uint64, error) {
	localHeight, _ := d.local.GetChainInfo()
	remoteHeight, _ := d.remote.GetChainInfo()

	if remoteHeight == 0 {
		return localHeight, 0, xerrors.New("remote height is 0")
	}
	if remoteHeight > localHeight {
		return localHeight, remoteHeight, nil
	}

	return localHeight, remoteHeight, xerrors.New("remote height less than local height")
}
